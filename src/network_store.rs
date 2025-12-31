//! Network storage with ParityDB backend
//!
//! Production-ready implementation for peer and network information:
//! - Peer information storage
//! - Ban list management
//! - Network statistics
//! - Peer scoring and reputation
//! - Connection tracking

use crate::db::{ParityDatabase, CF_ACCOUNT_STATE};
use crate::error::{Error, Result};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

/// Peer information
///
/// Stores detailed information about a connected peer including
/// connection statistics, version, and performance metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    /// Peer ID (unique identifier)
    pub id: u32,
    /// Peer address (IP:port)
    pub addr: String,
    /// Local address from peer's perspective
    pub addrlocal: Option<String>,
    /// Services provided by peer (bitmask)
    pub services: u64,
    /// Timestamp of last send
    pub lastsend: u64,
    /// Timestamp of last receive
    pub lastrecv: u64,
    /// Total bytes sent to peer
    pub bytessent: u64,
    /// Total bytes received from peer
    pub bytesrecv: u64,
    /// Connection time (Unix seconds)
    pub conntime: u64,
    /// Time offset from peer
    pub timeoffset: i64,
    /// Ping time in milliseconds
    pub pingtime: f64,
    /// Protocol version
    pub version: u32,
    /// User agent string
    pub subver: String,
    /// Whether connection is inbound
    pub inbound: bool,
    /// Starting block height
    pub startingheight: u64,
    /// Ban score (0-100)
    pub banscore: u32,
    /// Number of headers synced
    pub synced_headers: u64,
    /// Number of blocks synced
    pub synced_blocks: u64,
}

impl PeerInfo {
    /// Create a new peer info
    pub fn new(id: u32, addr: String) -> Self {
        Self {
            id,
            addr,
            addrlocal: None,
            services: 0,
            lastsend: 0,
            lastrecv: 0,
            bytessent: 0,
            bytesrecv: 0,
            conntime: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            timeoffset: 0,
            pingtime: 0.0,
            version: 0,
            subver: String::new(),
            inbound: false,
            startingheight: 0,
            banscore: 0,
            synced_headers: 0,
            synced_blocks: 0,
        }
    }

    /// Get the storage key for this peer
    pub fn storage_key(&self) -> String {
        format!("peer:{}", self.id)
    }

    /// Increment ban score
    pub fn add_ban_score(&mut self, score: u32) {
        self.banscore = self.banscore.saturating_add(score).min(100);
    }

    /// Check if peer should be banned
    pub fn should_ban(&self) -> bool {
        self.banscore >= 100
    }
}

/// Ban entry for banned peers
///
/// Stores information about banned peers including ban reason and duration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BanEntry {
    /// Address that is banned (IP or IP:port)
    pub address: String,
    /// Timestamp when ban expires (0 = permanent)
    pub banned_until: u64,
    /// Timestamp when ban was created
    pub ban_created: u64,
    /// Reason for ban
    pub ban_reason: String,
}

impl BanEntry {
    /// Create a new ban entry
    pub fn new(address: String, ban_reason: String) -> Self {
        Self {
            address,
            banned_until: 0, // Permanent ban
            ban_created: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            ban_reason,
        }
    }

    /// Create a temporary ban entry
    pub fn temporary(address: String, ban_reason: String, duration_secs: u64) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            address,
            banned_until: now + duration_secs,
            ban_created: now,
            ban_reason,
        }
    }

    /// Get the storage key for this ban
    pub fn storage_key(&self) -> String {
        format!("ban:{}", self.address)
    }

    /// Check if ban is still active
    pub fn is_active(&self) -> bool {
        if self.banned_until == 0 {
            true // Permanent ban
        } else {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now < self.banned_until
        }
    }
}

/// Network statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStats {
    /// Total number of connected peers
    pub peer_count: u32,
    /// Number of inbound connections
    pub inbound_count: u32,
    /// Number of outbound connections
    pub outbound_count: u32,
    /// Total bytes sent
    pub total_bytes_sent: u64,
    /// Total bytes received
    pub total_bytes_recv: u64,
    /// Average ping time
    pub avg_ping_time: f64,
    /// Number of banned peers
    pub banned_count: u32,
    /// Timestamp of last update
    pub last_update: u64,
}

/// Network Store with ParityDB backend
///
/// Provides persistent storage for network information with:
/// - Peer information storage
/// - Ban list management
/// - Network statistics
/// - Peer scoring and reputation
/// - Connection tracking
pub struct NetworkStore {
    db: Arc<ParityDatabase>,
    /// Cache for peer information
    peer_cache: Arc<RwLock<HashMap<u32, PeerInfo>>>,
    /// Cache for ban entries
    ban_cache: Arc<RwLock<HashMap<String, BanEntry>>>,
    /// Cache for network statistics
    stats_cache: Arc<RwLock<Option<NetworkStats>>>,
}

impl NetworkStore {
    /// Create new network store with ParityDB backend
    pub fn new(db: Arc<ParityDatabase>) -> Self {
        info!("Initializing NetworkStore with ParityDB backend");
        Self {
            db,
            peer_cache: Arc::new(RwLock::new(HashMap::new())),
            ban_cache: Arc::new(RwLock::new(HashMap::new())),
            stats_cache: Arc::new(RwLock::new(None)),
        }
    }

    /// Store peer information
    ///
    /// # Arguments
    /// * `peer` - Peer information to store
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if storage fails
    pub fn store_peer_info(&self, peer: &PeerInfo) -> Result<()> {
        debug!("Storing peer info: {} ({})", peer.id, peer.addr);

        // Serialize peer info
        let peer_data =
            serde_json::to_vec(peer).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db
            .put(CF_ACCOUNT_STATE, peer.storage_key().as_bytes(), &peer_data)?;

        // Update cache
        self.peer_cache.write().insert(peer.id, peer.clone());

        // Add to peer index
        let peer_index_key = "network:peer_index:all";
        let mut peer_list = self.get_peer_list()?;
        if !peer_list.contains(&peer.id) {
            peer_list.push(peer.id);
            let list_data = serde_json::to_vec(&peer_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db
                .put(CF_ACCOUNT_STATE, peer_index_key.as_bytes(), &list_data)?;
        }

        // Invalidate stats cache
        *self.stats_cache.write() = None;

        info!("Peer info stored: {} ({})", peer.id, peer.addr);
        Ok(())
    }

    /// Get peer information
    ///
    /// # Arguments
    /// * `peer_id` - Peer ID
    ///
    /// # Returns
    /// * `Ok(Some(PeerInfo))` if found
    /// * `Ok(None)` if not found
    /// * `Err(Error)` if retrieval fails
    pub fn get_peer_info(&self, peer_id: u32) -> Result<Option<PeerInfo>> {
        debug!("Retrieving peer info: {}", peer_id);

        // Check cache first
        if let Some(peer) = self.peer_cache.read().get(&peer_id) {
            return Ok(Some(peer.clone()));
        }

        // Query database
        let key = format!("peer:{}", peer_id);
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let peer: PeerInfo = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;

                // Update cache
                self.peer_cache.write().insert(peer_id, peer.clone());
                Ok(Some(peer))
            }
            None => Ok(None),
        }
    }

    /// List all peers
    ///
    /// # Returns
    /// * `Ok(Vec<PeerInfo>)` - List of all peers
    /// * `Err(Error)` if operation fails
    pub fn list_peers(&self) -> Result<Vec<PeerInfo>> {
        debug!("Listing all peers");

        let peer_ids = self.get_peer_list()?;
        let mut result = Vec::new();

        for peer_id in peer_ids {
            if let Some(peer) = self.get_peer_info(peer_id)? {
                result.push(peer);
            }
        }

        info!("Found {} peers", result.len());
        Ok(result)
    }

    /// Remove peer
    ///
    /// # Arguments
    /// * `peer_id` - Peer ID to remove
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if removal fails
    pub fn remove_peer(&self, peer_id: u32) -> Result<()> {
        debug!("Removing peer: {}", peer_id);

        let key = format!("peer:{}", peer_id);
        self.db.delete(CF_ACCOUNT_STATE, key.as_bytes())?;

        // Remove from cache
        self.peer_cache.write().remove(&peer_id);

        // Remove from peer index
        let peer_index_key = "network:peer_index:all";
        let mut peer_list = self.get_peer_list()?;
        peer_list.retain(|&id| id != peer_id);
        let list_data =
            serde_json::to_vec(&peer_list).map_err(|e| Error::SerializationError(e.to_string()))?;
        self.db
            .put(CF_ACCOUNT_STATE, peer_index_key.as_bytes(), &list_data)?;

        // Invalidate stats cache
        *self.stats_cache.write() = None;

        info!("Peer removed: {}", peer_id);
        Ok(())
    }

    /// Add a ban
    ///
    /// # Arguments
    /// * `ban` - Ban entry to add
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn add_ban(&self, ban: &BanEntry) -> Result<()> {
        debug!("Adding ban: {}", ban.address);

        // Serialize ban entry
        let ban_data =
            serde_json::to_vec(ban).map_err(|e| Error::SerializationError(e.to_string()))?;

        // Store to database
        self.db
            .put(CF_ACCOUNT_STATE, ban.storage_key().as_bytes(), &ban_data)?;

        // Update cache
        self.ban_cache
            .write()
            .insert(ban.address.clone(), ban.clone());

        // Add to ban index
        let ban_index_key = "network:ban_index:all";
        let mut ban_list = self.get_ban_list()?;
        if !ban_list.contains(&ban.address) {
            ban_list.push(ban.address.clone());
            let list_data = serde_json::to_vec(&ban_list)
                .map_err(|e| Error::SerializationError(e.to_string()))?;
            self.db
                .put(CF_ACCOUNT_STATE, ban_index_key.as_bytes(), &list_data)?;
        }

        // Invalidate stats cache
        *self.stats_cache.write() = None;

        info!("Ban added: {}", ban.address);
        Ok(())
    }

    /// Remove a ban
    ///
    /// # Arguments
    /// * `address` - Address to unban
    ///
    /// # Returns
    /// * `Ok(())` on success
    /// * `Err(Error)` if operation fails
    pub fn remove_ban(&self, address: &str) -> Result<()> {
        debug!("Removing ban: {}", address);

        let key = format!("ban:{}", address);
        self.db.delete(CF_ACCOUNT_STATE, key.as_bytes())?;

        // Remove from cache
        self.ban_cache.write().remove(address);

        // Remove from ban index
        let ban_index_key = "network:ban_index:all";
        let mut ban_list = self.get_ban_list()?;
        ban_list.retain(|a| a != address);
        let list_data =
            serde_json::to_vec(&ban_list).map_err(|e| Error::SerializationError(e.to_string()))?;
        self.db
            .put(CF_ACCOUNT_STATE, ban_index_key.as_bytes(), &list_data)?;

        // Invalidate stats cache
        *self.stats_cache.write() = None;

        info!("Ban removed: {}", address);
        Ok(())
    }

    /// List all bans
    ///
    /// # Returns
    /// * `Ok(Vec<BanEntry>)` - List of active bans
    /// * `Err(Error)` if operation fails
    pub fn list_bans(&self) -> Result<Vec<BanEntry>> {
        debug!("Listing all bans");

        let addresses = self.get_ban_list()?;
        let mut result = Vec::new();

        for address in addresses {
            if let Some(ban) = self.get_ban(&address)? {
                if ban.is_active() {
                    result.push(ban);
                }
            }
        }

        info!("Found {} active bans", result.len());
        Ok(result)
    }

    /// Check if address is banned
    ///
    /// # Arguments
    /// * `address` - Address to check
    ///
    /// # Returns
    /// * `Ok(true)` if banned
    /// * `Ok(false)` if not banned
    /// * `Err(Error)` if check fails
    pub fn is_banned(&self, address: &str) -> Result<bool> {
        if let Some(ban) = self.get_ban(address)? {
            Ok(ban.is_active())
        } else {
            Ok(false)
        }
    }

    /// Get network statistics
    ///
    /// # Returns
    /// * `Ok(NetworkStats)` - Network statistics
    /// * `Err(Error)` if operation fails
    pub fn get_network_stats(&self) -> Result<NetworkStats> {
        debug!("Getting network statistics");

        // Check cache first
        if let Some(cached_stats) = self.stats_cache.read().as_ref() {
            return Ok(cached_stats.clone());
        }

        // Calculate from current state
        let peers = self.list_peers()?;
        let bans = self.list_bans()?;

        let peer_count = peers.len() as u32;
        let inbound_count = peers.iter().filter(|p| p.inbound).count() as u32;
        let outbound_count = peer_count - inbound_count;

        let total_bytes_sent: u64 = peers.iter().map(|p| p.bytessent).sum();
        let total_bytes_recv: u64 = peers.iter().map(|p| p.bytesrecv).sum();

        let avg_ping_time = if !peers.is_empty() {
            peers.iter().map(|p| p.pingtime).sum::<f64>() / peers.len() as f64
        } else {
            0.0
        };

        let stats = NetworkStats {
            peer_count,
            inbound_count,
            outbound_count,
            total_bytes_sent,
            total_bytes_recv,
            avg_ping_time,
            banned_count: bans.len() as u32,
            last_update: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        // Cache the result
        *self.stats_cache.write() = Some(stats.clone());

        info!("Network stats: {} peers, {} bans", peer_count, bans.len());
        Ok(stats)
    }

    // Helper method to get peer list
    fn get_peer_list(&self) -> Result<Vec<u32>> {
        let peer_index_key = "network:peer_index:all";
        match self.db.get(CF_ACCOUNT_STATE, peer_index_key.as_bytes())? {
            Some(data) => {
                let list: Vec<u32> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }

    // Helper method to get ban list
    fn get_ban_list(&self) -> Result<Vec<String>> {
        let ban_index_key = "network:ban_index:all";
        match self.db.get(CF_ACCOUNT_STATE, ban_index_key.as_bytes())? {
            Some(data) => {
                let list: Vec<String> = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;
                Ok(list)
            }
            None => Ok(Vec::new()),
        }
    }

    // Helper method to get a ban entry
    fn get_ban(&self, address: &str) -> Result<Option<BanEntry>> {
        // Check cache first
        if let Some(ban) = self.ban_cache.read().get(address) {
            return Ok(Some(ban.clone()));
        }

        // Query database
        let key = format!("ban:{}", address);
        match self.db.get(CF_ACCOUNT_STATE, key.as_bytes())? {
            Some(data) => {
                let ban: BanEntry = serde_json::from_slice(&data)
                    .map_err(|e| Error::DeserializationError(e.to_string()))?;

                // Update cache
                self.ban_cache
                    .write()
                    .insert(address.to_string(), ban.clone());
                Ok(Some(ban))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_info_creation() {
        let peer = PeerInfo::new(1, "127.0.0.1:8333".to_string());
        assert_eq!(peer.id, 1);
        assert_eq!(peer.addr, "127.0.0.1:8333");
        assert_eq!(peer.banscore, 0);
    }

    #[test]
    fn test_peer_info_storage_key() {
        let peer = PeerInfo::new(1, "127.0.0.1:8333".to_string());
        assert_eq!(peer.storage_key(), "peer:1");
    }

    #[test]
    fn test_peer_ban_score() {
        let mut peer = PeerInfo::new(1, "127.0.0.1:8333".to_string());
        peer.add_ban_score(50);
        assert_eq!(peer.banscore, 50);
        assert_eq!(peer.should_ban(), false);

        peer.add_ban_score(50);
        assert_eq!(peer.banscore, 100);
        assert_eq!(peer.should_ban(), true);
    }

    #[test]
    fn test_ban_entry_creation() {
        let ban = BanEntry::new("127.0.0.1".to_string(), "spam".to_string());
        assert_eq!(ban.address, "127.0.0.1");
        assert_eq!(ban.ban_reason, "spam");
        assert_eq!(ban.banned_until, 0); // Permanent
        assert_eq!(ban.is_active(), true);
    }

    #[test]
    fn test_ban_entry_storage_key() {
        let ban = BanEntry::new("127.0.0.1".to_string(), "spam".to_string());
        assert_eq!(ban.storage_key(), "ban:127.0.0.1");
    }

    #[test]
    fn test_temporary_ban() {
        let ban = BanEntry::temporary("127.0.0.1".to_string(), "spam".to_string(), 3600);
        assert_eq!(ban.address, "127.0.0.1");
        assert!(ban.banned_until > 0);
        assert_eq!(ban.is_active(), true);
    }
}
