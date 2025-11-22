//! Object ownership operations
//!
//! This module implements ownership-specific operations for blockchain objects,
//! enforcing access control and ownership rules according to the object model.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use silver_core::{Object, ObjectID, Owner, SequenceNumber, SilverAddress, TransactionDigest};
use tracing::{debug, error, warn};

/// Ownership operations for objects
///
/// Provides methods to enforce ownership rules and perform ownership-related operations.
pub struct OwnershipManager;

impl OwnershipManager {
    /// Create a new ownership manager
    pub fn new() -> Self {
        Self
    }

    /// Verify that an address can modify an owned object
    ///
    /// For owned objects, only the owner can modify them.
    ///
    /// # Arguments
    /// * `object` - The object to check
    /// * `signer` - The address attempting to modify the object
    ///
    /// # Returns
    /// - `Ok(())` if the signer can modify the object
    /// - `Err` if the signer cannot modify the object
    ///
    /// # Requirements
    /// Implements requirement 20.1: "THE Object Store SHALL support owned objects
    /// that can only be modified by transactions signed by the owner's private key"
    pub fn verify_owned_object_access(
        &self,
        object: &Object,
        signer: &SilverAddress,
    ) -> Result<()> {
        debug!(
            "Verifying owned object access: object={}, signer={}",
            object.id, signer
        );

        // Check if object is address-owned
        if !object.owner.is_address_owned() {
            error!(
                "Object {} is not address-owned (owner: {})",
                object.id, object.owner
            );
            return Err(Error::InvalidData(format!(
                "Object {} is not address-owned",
                object.id
            )));
        }

        // Get the owner address
        let owner_addr = object.owner.address().ok_or_else(|| {
            Error::InvalidData(format!("Object {} has no owner address", object.id))
        })?;

        // Verify signer matches owner
        if owner_addr != signer {
            warn!(
                "Access denied: object {} owned by {}, attempted by {}",
                object.id, owner_addr, signer
            );
            return Err(Error::PermissionDenied(format!(
                "Object {} is owned by {}, cannot be modified by {}",
                object.id, owner_addr, signer
            )));
        }

        debug!(
            "Access granted: signer {} can modify object {}",
            signer, object.id
        );
        Ok(())
    }

    /// Verify that a transaction can modify an owned object
    ///
    /// This checks both ownership and signature validity.
    ///
    /// # Arguments
    /// * `object` - The object to modify
    /// * `transaction_signer` - The address that signed the transaction
    ///
    /// # Returns
    /// - `Ok(())` if modification is allowed
    /// - `Err` if modification is not allowed
    pub fn verify_modification_permission(
        &self,
        object: &Object,
        transaction_signer: &SilverAddress,
    ) -> Result<()> {
        // For owned objects, verify the signer is the owner
        if object.owner.is_address_owned() {
            return self.verify_owned_object_access(object, transaction_signer);
        }

        // For other ownership types, use different rules
        match &object.owner {
            Owner::Immutable => {
                error!("Cannot modify immutable object {}", object.id);
                Err(Error::PermissionDenied(format!(
                    "Object {} is immutable and cannot be modified",
                    object.id
                )))
            }
            Owner::Shared { .. } => {
                // Shared objects can be modified by anyone (with consensus)
                debug!(
                    "Shared object {} can be modified by {}",
                    object.id, transaction_signer
                );
                Ok(())
            }
            Owner::ObjectOwner(parent_id) => {
                // Wrapped objects must be modified through parent
                error!(
                    "Cannot directly modify wrapped object {} (parent: {})",
                    object.id, parent_id
                );
                Err(Error::PermissionDenied(format!(
                    "Object {} is wrapped in parent {}, must modify through parent",
                    object.id, parent_id
                )))
            }
            Owner::AddressOwner(_) => {
                // Already handled above
                unreachable!()
            }
        }
    }

    /// Transfer ownership of an owned object to a new address
    ///
    /// This creates a new version of the object with updated ownership.
    ///
    /// # Arguments
    /// * `object` - The object to transfer
    /// * `current_owner` - The current owner (must match object owner)
    /// * `new_owner` - The new owner address
    /// * `transaction_digest` - The transaction performing the transfer
    ///
    /// # Returns
    /// New version of the object with updated ownership
    ///
    /// # Errors
    /// Returns error if:
    /// - Object is not address-owned
    /// - Current owner doesn't match
    /// - Object is immutable
    pub fn transfer_ownership(
        &self,
        object: &Object,
        current_owner: &SilverAddress,
        new_owner: SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<Object> {
        debug!(
            "Transferring ownership: object={}, from={}, to={}",
            object.id, current_owner, new_owner
        );

        // Verify current owner can transfer
        self.verify_owned_object_access(object, current_owner)?;

        // Perform the transfer
        let new_object = object.transfer_to(new_owner, transaction_digest)?;

        debug!(
            "Ownership transferred: object={} v{} -> v{}",
            object.id, object.version, new_object.version
        );

        Ok(new_object)
    }

    /// Validate that an object can be used as input to a transaction
    ///
    /// This checks ownership and version consistency.
    ///
    /// # Arguments
    /// * `object` - The object to validate
    /// * `expected_version` - The expected version number
    /// * `transaction_signer` - The address signing the transaction
    ///
    /// # Returns
    /// - `Ok(())` if object can be used
    /// - `Err` if object cannot be used
    pub fn validate_object_input(
        &self,
        object: &Object,
        expected_version: u64,
        transaction_signer: &SilverAddress,
    ) -> Result<()> {
        debug!(
            "Validating object input: object={}, expected_version={}, signer={}",
            object.id, expected_version, transaction_signer
        );

        // Check version matches
        if object.version.value() != expected_version {
            error!(
                "Version mismatch: object {} has version {}, expected {}",
                object.id,
                object.version.value(),
                expected_version
            );
            return Err(Error::InvalidData(format!(
                "Object {} version mismatch: expected {}, got {}",
                object.id,
                expected_version,
                object.version.value()
            )));
        }

        // For owned objects, verify signer is owner
        if object.owner.is_address_owned() {
            self.verify_owned_object_access(object, transaction_signer)?;
        }

        debug!("Object input validation passed: {}", object.id);
        Ok(())
    }

    /// Check if an object can be deleted by the given address
    ///
    /// # Arguments
    /// * `object` - The object to check
    /// * `signer` - The address attempting to delete
    ///
    /// # Returns
    /// - `Ok(())` if deletion is allowed
    /// - `Err` if deletion is not allowed
    pub fn verify_deletion_permission(
        &self,
        object: &Object,
        signer: &SilverAddress,
    ) -> Result<()> {
        debug!(
            "Verifying deletion permission: object={}, signer={}",
            object.id, signer
        );

        match &object.owner {
            Owner::AddressOwner(owner) => {
                if owner != signer {
                    return Err(Error::PermissionDenied(format!(
                        "Object {} is owned by {}, cannot be deleted by {}",
                        object.id, owner, signer
                    )));
                }
                Ok(())
            }
            Owner::Immutable => Err(Error::PermissionDenied(format!(
                "Object {} is immutable and cannot be deleted",
                object.id
            ))),
            Owner::Shared { .. } => {
                // Shared objects typically cannot be deleted
                Err(Error::PermissionDenied(format!(
                    "Shared object {} cannot be deleted",
                    object.id
                )))
            }
            Owner::ObjectOwner(parent_id) => Err(Error::PermissionDenied(format!(
                "Wrapped object {} must be deleted through parent {}",
                object.id, parent_id
            ))),
        }
    }

    /// Get all objects owned by an address that can be modified
    ///
    /// This filters out immutable and wrapped objects.
    ///
    /// # Arguments
    /// * `objects` - List of objects to filter
    /// * `owner` - The owner address
    ///
    /// # Returns
    /// Vector of objects that can be modified by the owner
    pub fn filter_modifiable_objects<'a>(
        &self,
        objects: &'a [Object],
        owner: &SilverAddress,
    ) -> Vec<&'a Object> {
        objects
            .iter()
            .filter(|obj| {
                obj.owner.is_address_owned()
                    && obj.owner.address() == Some(owner)
                    && !obj.owner.is_immutable()
            })
            .collect()
    }
}

impl Default for OwnershipManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared object operations manager
///
/// Handles operations specific to shared objects that require consensus ordering.
pub struct SharedObjectManager;

impl SharedObjectManager {
    /// Create a new shared object manager
    pub fn new() -> Self {
        Self
    }

    /// Verify that an object is shared
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// - `Ok(())` if object is shared
    /// - `Err` if object is not shared
    pub fn verify_is_shared(&self, object: &Object) -> Result<()> {
        if !object.owner.is_shared() {
            return Err(Error::InvalidData(format!(
                "Object {} is not a shared object",
                object.id
            )));
        }
        Ok(())
    }

    /// Check if a transaction can access a shared object
    ///
    /// Shared objects can be accessed by any transaction, but modifications
    /// require consensus ordering to prevent conflicts.
    ///
    /// # Arguments
    /// * `object` - The shared object
    /// * `_transaction_signer` - The address signing the transaction (not used for shared objects)
    ///
    /// # Returns
    /// - `Ok(())` if access is allowed
    /// - `Err` if object is not shared
    ///
    /// # Requirements
    /// Implements requirement 20.2: "THE Object Store SHALL support shared objects
    /// that can be accessed by any transaction subject to consensus ordering"
    pub fn verify_shared_object_access(
        &self,
        object: &Object,
        _transaction_signer: &SilverAddress,
    ) -> Result<()> {
        debug!(
            "Verifying shared object access: object={}",
            object.id
        );

        self.verify_is_shared(object)?;

        // Shared objects can be accessed by anyone
        debug!("Access granted: shared object {} can be accessed by any transaction", object.id);
        Ok(())
    }

    /// Convert an owned object to a shared object
    ///
    /// This makes the object accessible by any transaction.
    ///
    /// # Arguments
    /// * `object` - The object to make shared
    /// * `owner` - The current owner (must match object owner)
    /// * `transaction_digest` - The transaction performing the conversion
    ///
    /// # Returns
    /// New version of the object as a shared object
    ///
    /// # Errors
    /// Returns error if:
    /// - Object is not address-owned
    /// - Owner doesn't match
    pub fn make_shared(
        &self,
        object: &Object,
        owner: &SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<Object> {
        debug!(
            "Making object shared: object={}, owner={}",
            object.id, owner
        );

        // Verify owner can make it shared
        if !object.owner.is_address_owned() {
            return Err(Error::InvalidData(format!(
                "Only address-owned objects can be made shared, object {} has owner: {}",
                object.id, object.owner
            )));
        }

        let owner_addr = object.owner.address().ok_or_else(|| {
            Error::InvalidData(format!("Object {} has no owner address", object.id))
        })?;

        if owner_addr != owner {
            return Err(Error::PermissionDenied(format!(
                "Object {} is owned by {}, cannot be shared by {}",
                object.id, owner_addr, owner
            )));
        }

        // Make it shared
        let shared_object = object.make_shared(transaction_digest)?;

        debug!(
            "Object {} v{} is now shared (initial_shared_version: {})",
            shared_object.id,
            shared_object.version,
            shared_object.version
        );

        Ok(shared_object)
    }

    /// Modify a shared object
    ///
    /// Creates a new version of the shared object with updated data.
    /// This operation requires consensus ordering to prevent conflicts.
    ///
    /// # Arguments
    /// * `object` - The shared object to modify
    /// * `new_data` - The new data for the object
    /// * `transaction_digest` - The transaction performing the modification
    ///
    /// # Returns
    /// New version of the shared object
    ///
    /// # Errors
    /// Returns error if object is not shared
    pub fn modify_shared_object(
        &self,
        object: &Object,
        new_data: Vec<u8>,
        transaction_digest: TransactionDigest,
    ) -> Result<Object> {
        debug!(
            "Modifying shared object: object={} v{}",
            object.id, object.version
        );

        self.verify_is_shared(object)?;

        // Create new version
        let new_object = object.new_version(new_data, transaction_digest)?;

        debug!(
            "Shared object modified: {} v{} -> v{}",
            object.id, object.version, new_object.version
        );

        Ok(new_object)
    }

    /// Validate shared object for transaction input
    ///
    /// Checks that the object is shared and version matches.
    ///
    /// # Arguments
    /// * `object` - The shared object
    /// * `expected_version` - The expected version number
    ///
    /// # Returns
    /// - `Ok(())` if validation passes
    /// - `Err` if validation fails
    pub fn validate_shared_object_input(
        &self,
        object: &Object,
        expected_version: u64,
    ) -> Result<()> {
        debug!(
            "Validating shared object input: object={}, expected_version={}",
            object.id, expected_version
        );

        self.verify_is_shared(object)?;

        // Check version matches
        if object.version.value() != expected_version {
            return Err(Error::InvalidData(format!(
                "Shared object {} version mismatch: expected {}, got {}",
                object.id,
                expected_version,
                object.version.value()
            )));
        }

        debug!("Shared object input validation passed: {}", object.id);
        Ok(())
    }

    /// Check if an object requires consensus ordering
    ///
    /// Shared objects require consensus ordering for modifications.
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// `true` if object requires consensus ordering, `false` otherwise
    pub fn requires_consensus_ordering(&self, object: &Object) -> bool {
        object.owner.is_shared()
    }

    /// Get the initial shared version of a shared object
    ///
    /// # Arguments
    /// * `object` - The shared object
    ///
    /// # Returns
    /// The initial shared version, or None if object is not shared
    pub fn get_initial_shared_version(&self, object: &Object) -> Option<SequenceNumber> {
        match &object.owner {
            Owner::Shared {
                initial_shared_version,
            } => Some(*initial_shared_version),
            _ => None,
        }
    }
}

impl Default for SharedObjectManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Immutable object operations manager
///
/// Handles operations specific to immutable objects that cannot be modified.
pub struct ImmutableObjectManager;

impl ImmutableObjectManager {
    /// Create a new immutable object manager
    pub fn new() -> Self {
        Self
    }

    /// Verify that an object is immutable
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// - `Ok(())` if object is immutable
    /// - `Err` if object is not immutable
    pub fn verify_is_immutable(&self, object: &Object) -> Result<()> {
        if !object.owner.is_immutable() {
            return Err(Error::InvalidData(format!(
                "Object {} is not immutable",
                object.id
            )));
        }
        Ok(())
    }

    /// Check if an object can be read without consensus
    ///
    /// Immutable objects can be read without consensus since they never change.
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// `true` if object can be read without consensus, `false` otherwise
    ///
    /// # Requirements
    /// Implements requirement 20.3: "THE Object Store SHALL support immutable objects
    /// that cannot be modified after creation and can be read without consensus"
    pub fn can_read_without_consensus(&self, object: &Object) -> bool {
        debug!(
            "Checking if object {} can be read without consensus",
            object.id
        );

        let result = object.owner.is_immutable();

        debug!(
            "Object {} can_read_without_consensus: {}",
            object.id, result
        );

        result
    }

    /// Convert an owned object to an immutable object
    ///
    /// This freezes the object, making it permanently read-only.
    ///
    /// # Arguments
    /// * `object` - The object to make immutable
    /// * `owner` - The current owner (must match object owner)
    /// * `transaction_digest` - The transaction performing the conversion
    ///
    /// # Returns
    /// New version of the object as an immutable object
    ///
    /// # Errors
    /// Returns error if:
    /// - Object is not address-owned
    /// - Owner doesn't match
    pub fn make_immutable(
        &self,
        object: &Object,
        owner: &SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<Object> {
        debug!(
            "Making object immutable: object={}, owner={}",
            object.id, owner
        );

        // Verify owner can make it immutable
        if !object.owner.is_address_owned() {
            return Err(Error::InvalidData(format!(
                "Only address-owned objects can be made immutable, object {} has owner: {}",
                object.id, object.owner
            )));
        }

        let owner_addr = object.owner.address().ok_or_else(|| {
            Error::InvalidData(format!("Object {} has no owner address", object.id))
        })?;

        if owner_addr != owner {
            return Err(Error::PermissionDenied(format!(
                "Object {} is owned by {}, cannot be frozen by {}",
                object.id, owner_addr, owner
            )));
        }

        // Make it immutable
        let immutable_object = object.make_immutable(transaction_digest)?;

        debug!(
            "Object {} v{} is now immutable",
            immutable_object.id, immutable_object.version
        );

        Ok(immutable_object)
    }

    /// Verify that an immutable object cannot be modified
    ///
    /// This always returns an error since immutable objects cannot be modified.
    ///
    /// # Arguments
    /// * `object` - The immutable object
    ///
    /// # Returns
    /// Always returns `Err` since immutable objects cannot be modified
    pub fn verify_cannot_modify(&self, object: &Object) -> Result<()> {
        self.verify_is_immutable(object)?;

        error!("Attempt to modify immutable object {}", object.id);
        Err(Error::PermissionDenied(format!(
            "Object {} is immutable and cannot be modified",
            object.id
        )))
    }

    /// Validate immutable object for transaction input (read-only)
    ///
    /// Immutable objects can be used as inputs for read-only operations.
    ///
    /// # Arguments
    /// * `object` - The immutable object
    ///
    /// # Returns
    /// - `Ok(())` if validation passes
    /// - `Err` if object is not immutable
    pub fn validate_immutable_object_input(&self, object: &Object) -> Result<()> {
        debug!(
            "Validating immutable object input: object={}",
            object.id
        );

        self.verify_is_immutable(object)?;

        debug!("Immutable object input validation passed: {}", object.id);
        Ok(())
    }

    /// Check if an object is permanently frozen
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// `true` if object is immutable, `false` otherwise
    pub fn is_frozen(&self, object: &Object) -> bool {
        object.owner.is_immutable()
    }

    /// Filter objects to get only immutable ones
    ///
    /// # Arguments
    /// * `objects` - List of objects to filter
    ///
    /// # Returns
    /// Vector of immutable objects
    pub fn filter_immutable_objects<'a>(&self, objects: &'a [Object]) -> Vec<&'a Object> {
        objects
            .iter()
            .filter(|obj| obj.owner.is_immutable())
            .collect()
    }
}

impl Default for ImmutableObjectManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Wrapped object operations manager
///
/// Handles operations specific to wrapped objects (objects contained within other objects).
pub struct WrappedObjectManager;

impl WrappedObjectManager {
    /// Create a new wrapped object manager
    pub fn new() -> Self {
        Self
    }

    /// Verify that an object is wrapped
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// - `Ok(parent_id)` if object is wrapped, returning the parent object ID
    /// - `Err` if object is not wrapped
    pub fn verify_is_wrapped(&self, object: &Object) -> Result<ObjectID> {
        match &object.owner {
            Owner::ObjectOwner(parent_id) => Ok(*parent_id),
            _ => Err(Error::InvalidData(format!(
                "Object {} is not a wrapped object",
                object.id
            ))),
        }
    }

    /// Get the parent object ID of a wrapped object
    ///
    /// # Arguments
    /// * `object` - The wrapped object
    ///
    /// # Returns
    /// The parent object ID, or None if object is not wrapped
    ///
    /// # Requirements
    /// Implements requirement 20.4: "THE Object Store SHALL support wrapped objects
    /// that are contained within other objects and inherit the parent's ownership model"
    pub fn get_parent_object(&self, object: &Object) -> Option<ObjectID> {
        debug!(
            "Getting parent object for wrapped object: {}",
            object.id
        );

        match &object.owner {
            Owner::ObjectOwner(parent_id) => {
                debug!("Object {} is wrapped in parent {}", object.id, parent_id);
                Some(*parent_id)
            }
            _ => {
                debug!("Object {} is not wrapped", object.id);
                None
            }
        }
    }

    /// Wrap an object inside another object
    ///
    /// This makes the object owned by another object, inheriting the parent's ownership model.
    ///
    /// # Arguments
    /// * `object` - The object to wrap
    /// * `parent_id` - The parent object ID
    /// * `owner` - The current owner (must match object owner)
    /// * `transaction_digest` - The transaction performing the wrapping
    ///
    /// # Returns
    /// New version of the object as a wrapped object
    ///
    /// # Errors
    /// Returns error if:
    /// - Object is not address-owned
    /// - Owner doesn't match
    pub fn wrap_object(
        &self,
        object: &Object,
        parent_id: ObjectID,
        owner: &SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<Object> {
        debug!(
            "Wrapping object: object={}, parent={}, owner={}",
            object.id, parent_id, owner
        );

        // Verify owner can wrap it
        if !object.owner.is_address_owned() {
            return Err(Error::InvalidData(format!(
                "Only address-owned objects can be wrapped, object {} has owner: {}",
                object.id, object.owner
            )));
        }

        let owner_addr = object.owner.address().ok_or_else(|| {
            Error::InvalidData(format!("Object {} has no owner address", object.id))
        })?;

        if owner_addr != owner {
            return Err(Error::PermissionDenied(format!(
                "Object {} is owned by {}, cannot be wrapped by {}",
                object.id, owner_addr, owner
            )));
        }

        // Create wrapped version
        let wrapped_object = Object::new(
            object.id,
            object.version.next(),
            Owner::ObjectOwner(parent_id),
            object.object_type.clone(),
            object.data.clone(),
            transaction_digest,
            object.storage_rebate,
        );

        debug!(
            "Object {} v{} is now wrapped in parent {}",
            wrapped_object.id, wrapped_object.version, parent_id
        );

        Ok(wrapped_object)
    }

    /// Verify that a wrapped object can only be modified through its parent
    ///
    /// This always returns an error since wrapped objects cannot be directly modified.
    ///
    /// # Arguments
    /// * `object` - The wrapped object
    ///
    /// # Returns
    /// Always returns `Err` with the parent object ID
    pub fn verify_must_modify_through_parent(&self, object: &Object) -> Result<()> {
        let parent_id = self.verify_is_wrapped(object)?;

        error!(
            "Attempt to directly modify wrapped object {} (parent: {})",
            object.id, parent_id
        );

        Err(Error::PermissionDenied(format!(
            "Object {} is wrapped in parent {}, must modify through parent",
            object.id, parent_id
        )))
    }

    /// Check if an object inherits ownership from a parent
    ///
    /// # Arguments
    /// * `object` - The object to check
    ///
    /// # Returns
    /// `true` if object is wrapped and inherits parent ownership, `false` otherwise
    pub fn inherits_parent_ownership(&self, object: &Object) -> bool {
        object.owner.is_object_owned()
    }

    /// Validate wrapped object for transaction input
    ///
    /// Wrapped objects can only be used as inputs when accessed through their parent.
    ///
    /// # Arguments
    /// * `object` - The wrapped object
    /// * `parent_id` - The expected parent object ID
    ///
    /// # Returns
    /// - `Ok(())` if validation passes
    /// - `Err` if validation fails
    pub fn validate_wrapped_object_input(
        &self,
        object: &Object,
        parent_id: &ObjectID,
    ) -> Result<()> {
        debug!(
            "Validating wrapped object input: object={}, expected_parent={}",
            object.id, parent_id
        );

        let actual_parent = self.verify_is_wrapped(object)?;

        if &actual_parent != parent_id {
            return Err(Error::InvalidData(format!(
                "Wrapped object {} parent mismatch: expected {}, got {}",
                object.id, parent_id, actual_parent
            )));
        }

        debug!("Wrapped object input validation passed: {}", object.id);
        Ok(())
    }

    /// Unwrap an object from its parent
    ///
    /// This converts a wrapped object back to an address-owned object.
    ///
    /// # Arguments
    /// * `object` - The wrapped object
    /// * `new_owner` - The new owner address
    /// * `transaction_digest` - The transaction performing the unwrapping
    ///
    /// # Returns
    /// New version of the object as an address-owned object
    ///
    /// # Errors
    /// Returns error if object is not wrapped
    pub fn unwrap_object(
        &self,
        object: &Object,
        new_owner: SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<Object> {
        debug!(
            "Unwrapping object: object={}, new_owner={}",
            object.id, new_owner
        );

        // Verify it's wrapped
        let parent_id = self.verify_is_wrapped(object)?;

        // Create unwrapped version
        let unwrapped_object = Object::new(
            object.id,
            object.version.next(),
            Owner::AddressOwner(new_owner),
            object.object_type.clone(),
            object.data.clone(),
            transaction_digest,
            object.storage_rebate,
        );

        debug!(
            "Object {} v{} unwrapped from parent {} to owner {}",
            unwrapped_object.id, unwrapped_object.version, parent_id, new_owner
        );

        Ok(unwrapped_object)
    }

    /// Filter objects to get only wrapped ones
    ///
    /// # Arguments
    /// * `objects` - List of objects to filter
    ///
    /// # Returns
    /// Vector of wrapped objects
    pub fn filter_wrapped_objects<'a>(&self, objects: &'a [Object]) -> Vec<&'a Object> {
        objects
            .iter()
            .filter(|obj| obj.owner.is_object_owned())
            .collect()
    }

    /// Get all objects wrapped by a specific parent
    ///
    /// # Arguments
    /// * `objects` - List of objects to filter
    /// * `parent_id` - The parent object ID
    ///
    /// # Returns
    /// Vector of objects wrapped by the specified parent
    pub fn filter_by_parent<'a>(
        &self,
        objects: &'a [Object],
        parent_id: &ObjectID,
    ) -> Vec<&'a Object> {
        objects
            .iter()
            .filter(|obj| match &obj.owner {
                Owner::ObjectOwner(pid) => pid == parent_id,
                _ => false,
            })
            .collect()
    }
}

impl Default for WrappedObjectManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Ownership transfer manager
///
/// Handles ownership transfer operations with event emission.
pub struct OwnershipTransferManager;

impl OwnershipTransferManager {
    /// Create a new ownership transfer manager
    pub fn new() -> Self {
        Self
    }

    /// Transfer ownership of an object to a new address
    ///
    /// This creates a new version of the object with updated ownership
    /// and emits an ownership transfer event.
    ///
    /// # Arguments
    /// * `object` - The object to transfer
    /// * `current_owner` - The current owner (must match object owner)
    /// * `new_owner` - The new owner address
    /// * `transaction_digest` - The transaction performing the transfer
    ///
    /// # Returns
    /// Tuple of (new object, transfer event data)
    ///
    /// # Errors
    /// Returns error if:
    /// - Object is not address-owned
    /// - Current owner doesn't match
    /// - Object is immutable or shared
    ///
    /// # Requirements
    /// Implements requirement 20.5: "WHEN an object ownership changes,
    /// THE Object Store SHALL update ownership metadata and emit an ownership transfer event"
    pub fn transfer_ownership(
        &self,
        object: &Object,
        current_owner: &SilverAddress,
        new_owner: SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<(Object, OwnershipTransferEvent)> {
        debug!(
            "Transferring ownership: object={}, from={}, to={}",
            object.id, current_owner, new_owner
        );

        // Verify object can be transferred
        if !object.owner.is_address_owned() {
            return Err(Error::InvalidData(format!(
                "Only address-owned objects can be transferred, object {} has owner: {}",
                object.id, object.owner
            )));
        }

        // Verify current owner matches
        let owner_addr = object.owner.address().ok_or_else(|| {
            Error::InvalidData(format!("Object {} has no owner address", object.id))
        })?;

        if owner_addr != current_owner {
            return Err(Error::PermissionDenied(format!(
                "Object {} is owned by {}, cannot be transferred by {}",
                object.id, owner_addr, current_owner
            )));
        }

        // Perform the transfer
        let new_object = object.transfer_to(new_owner, transaction_digest)?;

        // Create transfer event
        let event = OwnershipTransferEvent {
            object_id: object.id,
            old_owner: *current_owner,
            new_owner,
            old_version: object.version,
            new_version: new_object.version,
            transaction_digest,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        debug!(
            "Ownership transferred: object={} v{} -> v{}, from={} to={}",
            object.id, object.version, new_object.version, current_owner, new_owner
        );

        Ok((new_object, event))
    }

    /// Batch transfer multiple objects
    ///
    /// Transfers ownership of multiple objects atomically.
    ///
    /// # Arguments
    /// * `objects` - List of objects to transfer
    /// * `current_owner` - The current owner (must match all objects)
    /// * `new_owner` - The new owner address
    /// * `transaction_digest` - The transaction performing the transfers
    ///
    /// # Returns
    /// Vector of tuples (new object, transfer event)
    ///
    /// # Errors
    /// Returns error if any transfer fails. No transfers are performed on error.
    pub fn batch_transfer_ownership(
        &self,
        objects: &[Object],
        current_owner: &SilverAddress,
        new_owner: SilverAddress,
        transaction_digest: TransactionDigest,
    ) -> Result<Vec<(Object, OwnershipTransferEvent)>> {
        debug!(
            "Batch transferring {} objects from {} to {}",
            objects.len(),
            current_owner,
            new_owner
        );

        let mut results = Vec::with_capacity(objects.len());

        for object in objects {
            let (new_object, event) =
                self.transfer_ownership(object, current_owner, new_owner, transaction_digest)?;
            results.push((new_object, event));
        }

        debug!(
            "Batch transferred {} objects successfully",
            results.len()
        );

        Ok(results)
    }

    /// Validate that an ownership transfer is allowed
    ///
    /// # Arguments
    /// * `object` - The object to transfer
    /// * `current_owner` - The current owner
    /// * `new_owner` - The proposed new owner
    ///
    /// # Returns
    /// - `Ok(())` if transfer is allowed
    /// - `Err` if transfer is not allowed
    pub fn validate_transfer(
        &self,
        object: &Object,
        current_owner: &SilverAddress,
        new_owner: &SilverAddress,
    ) -> Result<()> {
        debug!(
            "Validating ownership transfer: object={}, from={}, to={}",
            object.id, current_owner, new_owner
        );

        // Check if object is transferable
        if !object.owner.is_address_owned() {
            return Err(Error::InvalidData(format!(
                "Object {} cannot be transferred (owner: {})",
                object.id, object.owner
            )));
        }

        // Verify current owner
        let owner_addr = object.owner.address().ok_or_else(|| {
            Error::InvalidData(format!("Object {} has no owner address", object.id))
        })?;

        if owner_addr != current_owner {
            return Err(Error::PermissionDenied(format!(
                "Object {} is owned by {}, not {}",
                object.id, owner_addr, current_owner
            )));
        }

        // Verify new owner is different
        if current_owner == new_owner {
            return Err(Error::InvalidData(format!(
                "Cannot transfer object {} to the same owner",
                object.id
            )));
        }

        debug!("Transfer validation passed for object {}", object.id);
        Ok(())
    }

    /// Get transfer history for an object
    ///
    /// Queries the event store for all transfer events related to an object.
    ///
    /// # Arguments
    /// * `object_id` - The object ID
    ///
    /// # Returns
    /// Vector of transfer events sorted by timestamp
    pub fn get_transfer_history(&self, object_id: &ObjectID) -> Vec<OwnershipTransferEvent> {
        // Query EventStore for transfer events
        let mut events = Vec::new();

        // Get all transfers for this object from the transfer map
        if let Some(transfers) = self.transfers.get(object_id) {
            for transfer in transfers.iter() {
                events.push(OwnershipTransferEvent {
                    object_id: transfer.object_id.clone(),
                    from_owner: transfer.from_owner.clone(),
                    to_owner: transfer.to_owner.clone(),
                    timestamp: transfer.timestamp,
                    transaction_digest: transfer.transaction_digest.clone(),
                });
            }
        }

        // Sort by timestamp (oldest first)
        events.sort_by_key(|e| e.timestamp);

        events
    }
}

impl Default for OwnershipTransferManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Ownership transfer event data
///
/// Contains information about an ownership transfer for event emission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OwnershipTransferEvent {
    /// The object that was transferred
    pub object_id: ObjectID,
    
    /// The previous owner
    pub old_owner: SilverAddress,
    
    /// The new owner
    pub new_owner: SilverAddress,
    
    /// The object version before transfer
    pub old_version: SequenceNumber,
    
    /// The object version after transfer
    pub new_version: SequenceNumber,
    
    /// The transaction that performed the transfer
    pub transaction_digest: TransactionDigest,
    
    /// Timestamp of the transfer (Unix milliseconds)
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use silver_core::{object::ObjectType, SequenceNumber};

    fn create_test_object(owner: Owner) -> Object {
        Object::new(
            ObjectID::new([1u8; 64]),
            SequenceNumber::new(0),
            owner,
            ObjectType::Coin,
            vec![1, 2, 3, 4],
            TransactionDigest::new([0u8; 64]),
            1000,
        )
    }

    #[test]
    fn test_verify_owned_object_access_success() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Owner should have access
        assert!(manager.verify_owned_object_access(&object, &owner).is_ok());
    }

    #[test]
    fn test_verify_owned_object_access_denied() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let other = SilverAddress::new([20u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Other address should not have access
        assert!(manager
            .verify_owned_object_access(&object, &other)
            .is_err());
    }

    #[test]
    fn test_verify_owned_object_access_not_owned() {
        let manager = OwnershipManager::new();
        let signer = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::Immutable);

        // Should fail for non-owned objects
        assert!(manager
            .verify_owned_object_access(&object, &signer)
            .is_err());
    }

    #[test]
    fn test_verify_modification_permission_owned() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Owner can modify
        assert!(manager
            .verify_modification_permission(&object, &owner)
            .is_ok());

        // Non-owner cannot modify
        let other = SilverAddress::new([20u8; 64]);
        assert!(manager
            .verify_modification_permission(&object, &other)
            .is_err());
    }

    #[test]
    fn test_verify_modification_permission_immutable() {
        let manager = OwnershipManager::new();
        let signer = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::Immutable);

        // Nobody can modify immutable objects
        assert!(manager
            .verify_modification_permission(&object, &signer)
            .is_err());
    }

    #[test]
    fn test_verify_modification_permission_shared() {
        let manager = OwnershipManager::new();
        let signer = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::Shared {
            initial_shared_version: SequenceNumber::new(0),
        });

        // Anyone can modify shared objects (with consensus)
        assert!(manager
            .verify_modification_permission(&object, &signer)
            .is_ok());
    }

    #[test]
    fn test_verify_modification_permission_wrapped() {
        let manager = OwnershipManager::new();
        let signer = SilverAddress::new([10u8; 64]);
        let parent_id = ObjectID::new([99u8; 64]);
        let object = create_test_object(Owner::ObjectOwner(parent_id));

        // Cannot directly modify wrapped objects
        assert!(manager
            .verify_modification_permission(&object, &signer)
            .is_err());
    }

    #[test]
    fn test_transfer_ownership_success() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let new_owner = SilverAddress::new([20u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));
        let tx_digest = TransactionDigest::new([1u8; 64]);

        // Transfer should succeed
        let new_object = manager
            .transfer_ownership(&object, &owner, new_owner, tx_digest)
            .unwrap();

        // Verify new ownership
        assert_eq!(new_object.owner.address(), Some(&new_owner));
        assert_eq!(new_object.version.value(), 1);
    }

    #[test]
    fn test_transfer_ownership_wrong_owner() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let wrong_owner = SilverAddress::new([30u8; 64]);
        let new_owner = SilverAddress::new([20u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));
        let tx_digest = TransactionDigest::new([1u8; 64]);

        // Transfer should fail with wrong owner
        assert!(manager
            .transfer_ownership(&object, &wrong_owner, new_owner, tx_digest)
            .is_err());
    }

    #[test]
    fn test_validate_object_input_success() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Validation should succeed with correct version and owner
        assert!(manager
            .validate_object_input(&object, 0, &owner)
            .is_ok());
    }

    #[test]
    fn test_validate_object_input_version_mismatch() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Validation should fail with wrong version
        assert!(manager
            .validate_object_input(&object, 1, &owner)
            .is_err());
    }

    #[test]
    fn test_validate_object_input_wrong_owner() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let other = SilverAddress::new([20u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Validation should fail with wrong owner
        assert!(manager
            .validate_object_input(&object, 0, &other)
            .is_err());
    }

    #[test]
    fn test_verify_deletion_permission_owned() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::AddressOwner(owner));

        // Owner can delete
        assert!(manager.verify_deletion_permission(&object, &owner).is_ok());

        // Non-owner cannot delete
        let other = SilverAddress::new([20u8; 64]);
        assert!(manager
            .verify_deletion_permission(&object, &other)
            .is_err());
    }

    #[test]
    fn test_verify_deletion_permission_immutable() {
        let manager = OwnershipManager::new();
        let signer = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::Immutable);

        // Cannot delete immutable objects
        assert!(manager
            .verify_deletion_permission(&object, &signer)
            .is_err());
    }

    #[test]
    fn test_verify_deletion_permission_shared() {
        let manager = OwnershipManager::new();
        let signer = SilverAddress::new([10u8; 64]);
        let object = create_test_object(Owner::Shared {
            initial_shared_version: SequenceNumber::new(0),
        });

        // Cannot delete shared objects
        assert!(manager
            .verify_deletion_permission(&object, &signer)
            .is_err());
    }

    #[test]
    fn test_filter_modifiable_objects() {
        let manager = OwnershipManager::new();
        let owner = SilverAddress::new([10u8; 64]);

        let objects = vec![
            create_test_object(Owner::AddressOwner(owner)),
            create_test_object(Owner::Immutable),
            create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            }),
            create_test_object(Owner::AddressOwner(owner)),
        ];

        let modifiable = manager.filter_modifiable_objects(&objects, &owner);

        // Should only return the 2 owned objects
        assert_eq!(modifiable.len(), 2);
    }

    // Shared object tests
    mod shared_tests {
        use super::*;

        #[test]
        fn test_verify_is_shared() {
            let manager = SharedObjectManager::new();

            let shared_obj = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });
            assert!(manager.verify_is_shared(&shared_obj).is_ok());

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(manager.verify_is_shared(&owned_obj).is_err());
        }

        #[test]
        fn test_verify_shared_object_access() {
            let manager = SharedObjectManager::new();
            let signer = SilverAddress::new([10u8; 64]);

            let shared_obj = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });

            // Any address can access shared objects
            assert!(manager
                .verify_shared_object_access(&shared_obj, &signer)
                .is_ok());

            let other_signer = SilverAddress::new([20u8; 64]);
            assert!(manager
                .verify_shared_object_access(&shared_obj, &other_signer)
                .is_ok());
        }

        #[test]
        fn test_verify_shared_object_access_not_shared() {
            let manager = SharedObjectManager::new();
            let signer = SilverAddress::new([10u8; 64]);

            let owned_obj = create_test_object(Owner::AddressOwner(signer));

            // Should fail for non-shared objects
            assert!(manager
                .verify_shared_object_access(&owned_obj, &signer)
                .is_err());
        }

        #[test]
        fn test_make_shared_success() {
            let manager = SharedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let shared_obj = manager.make_shared(&object, &owner, tx_digest).unwrap();

            assert!(shared_obj.owner.is_shared());
            assert_eq!(shared_obj.version.value(), 1);
        }

        #[test]
        fn test_make_shared_wrong_owner() {
            let manager = SharedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let wrong_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail with wrong owner
            assert!(manager.make_shared(&object, &wrong_owner, tx_digest).is_err());
        }

        #[test]
        fn test_make_shared_already_shared() {
            let manager = SharedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let object = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail if already shared
            assert!(manager.make_shared(&object, &owner, tx_digest).is_err());
        }

        #[test]
        fn test_modify_shared_object() {
            let manager = SharedObjectManager::new();
            let object = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });
            let tx_digest = TransactionDigest::new([1u8; 64]);
            let new_data = vec![5, 6, 7, 8];

            let modified = manager
                .modify_shared_object(&object, new_data.clone(), tx_digest)
                .unwrap();

            assert_eq!(modified.version.value(), 1);
            assert_eq!(modified.data, new_data);
            assert!(modified.owner.is_shared());
        }

        #[test]
        fn test_modify_shared_object_not_shared() {
            let manager = SharedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);
            let new_data = vec![5, 6, 7, 8];

            // Should fail for non-shared objects
            assert!(manager
                .modify_shared_object(&object, new_data, tx_digest)
                .is_err());
        }

        #[test]
        fn test_validate_shared_object_input() {
            let manager = SharedObjectManager::new();
            let object = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });

            // Should succeed with correct version
            assert!(manager.validate_shared_object_input(&object, 0).is_ok());

            // Should fail with wrong version
            assert!(manager.validate_shared_object_input(&object, 1).is_err());
        }

        #[test]
        fn test_requires_consensus_ordering() {
            let manager = SharedObjectManager::new();

            let shared_obj = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });
            assert!(manager.requires_consensus_ordering(&shared_obj));

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(!manager.requires_consensus_ordering(&owned_obj));

            let immutable_obj = create_test_object(Owner::Immutable);
            assert!(!manager.requires_consensus_ordering(&immutable_obj));
        }

        #[test]
        fn test_get_initial_shared_version() {
            let manager = SharedObjectManager::new();

            let shared_obj = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(5),
            });
            assert_eq!(
                manager.get_initial_shared_version(&shared_obj),
                Some(SequenceNumber::new(5))
            );

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert_eq!(manager.get_initial_shared_version(&owned_obj), None);
        }
    }

    // Immutable object tests
    mod immutable_tests {
        use super::*;

        #[test]
        fn test_verify_is_immutable() {
            let manager = ImmutableObjectManager::new();

            let immutable_obj = create_test_object(Owner::Immutable);
            assert!(manager.verify_is_immutable(&immutable_obj).is_ok());

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(manager.verify_is_immutable(&owned_obj).is_err());
        }

        #[test]
        fn test_can_read_without_consensus() {
            let manager = ImmutableObjectManager::new();

            let immutable_obj = create_test_object(Owner::Immutable);
            assert!(manager.can_read_without_consensus(&immutable_obj));

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(!manager.can_read_without_consensus(&owned_obj));

            let shared_obj = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });
            assert!(!manager.can_read_without_consensus(&shared_obj));
        }

        #[test]
        fn test_make_immutable_success() {
            let manager = ImmutableObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let immutable_obj = manager.make_immutable(&object, &owner, tx_digest).unwrap();

            assert!(immutable_obj.owner.is_immutable());
            assert_eq!(immutable_obj.version.value(), 1);
        }

        #[test]
        fn test_make_immutable_wrong_owner() {
            let manager = ImmutableObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let wrong_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail with wrong owner
            assert!(manager
                .make_immutable(&object, &wrong_owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_make_immutable_already_immutable() {
            let manager = ImmutableObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let object = create_test_object(Owner::Immutable);
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail if already immutable
            assert!(manager.make_immutable(&object, &owner, tx_digest).is_err());
        }

        #[test]
        fn test_verify_cannot_modify() {
            let manager = ImmutableObjectManager::new();

            let immutable_obj = create_test_object(Owner::Immutable);
            assert!(manager.verify_cannot_modify(&immutable_obj).is_err());

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(manager.verify_cannot_modify(&owned_obj).is_err());
        }

        #[test]
        fn test_validate_immutable_object_input() {
            let manager = ImmutableObjectManager::new();

            let immutable_obj = create_test_object(Owner::Immutable);
            assert!(manager
                .validate_immutable_object_input(&immutable_obj)
                .is_ok());

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(manager
                .validate_immutable_object_input(&owned_obj)
                .is_err());
        }

        #[test]
        fn test_is_frozen() {
            let manager = ImmutableObjectManager::new();

            let immutable_obj = create_test_object(Owner::Immutable);
            assert!(manager.is_frozen(&immutable_obj));

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(!manager.is_frozen(&owned_obj));
        }

        #[test]
        fn test_filter_immutable_objects() {
            let manager = ImmutableObjectManager::new();

            let objects = vec![
                create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64]))),
                create_test_object(Owner::Immutable),
                create_test_object(Owner::Shared {
                    initial_shared_version: SequenceNumber::new(0),
                }),
                create_test_object(Owner::Immutable),
            ];

            let immutable = manager.filter_immutable_objects(&objects);

            // Should only return the 2 immutable objects
            assert_eq!(immutable.len(), 2);
        }
    }

    // Wrapped object tests
    mod wrapped_tests {
        use super::*;

        #[test]
        fn test_verify_is_wrapped() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);

            let wrapped_obj = create_test_object(Owner::ObjectOwner(parent_id));
            assert_eq!(manager.verify_is_wrapped(&wrapped_obj).unwrap(), parent_id);

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(manager.verify_is_wrapped(&owned_obj).is_err());
        }

        #[test]
        fn test_get_parent_object() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);

            let wrapped_obj = create_test_object(Owner::ObjectOwner(parent_id));
            assert_eq!(manager.get_parent_object(&wrapped_obj), Some(parent_id));

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert_eq!(manager.get_parent_object(&owned_obj), None);
        }

        #[test]
        fn test_wrap_object_success() {
            let manager = WrappedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let parent_id = ObjectID::new([99u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let wrapped_obj = manager
                .wrap_object(&object, parent_id, &owner, tx_digest)
                .unwrap();

            assert!(wrapped_obj.owner.is_object_owned());
            assert_eq!(wrapped_obj.owner.parent_object(), Some(&parent_id));
            assert_eq!(wrapped_obj.version.value(), 1);
        }

        #[test]
        fn test_wrap_object_wrong_owner() {
            let manager = WrappedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let wrong_owner = SilverAddress::new([20u8; 64]);
            let parent_id = ObjectID::new([99u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail with wrong owner
            assert!(manager
                .wrap_object(&object, parent_id, &wrong_owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_wrap_object_already_wrapped() {
            let manager = WrappedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let parent_id = ObjectID::new([99u8; 64]);
            let object = create_test_object(Owner::ObjectOwner(parent_id));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail if already wrapped
            assert!(manager
                .wrap_object(&object, parent_id, &owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_verify_must_modify_through_parent() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);

            let wrapped_obj = create_test_object(Owner::ObjectOwner(parent_id));
            assert!(manager
                .verify_must_modify_through_parent(&wrapped_obj)
                .is_err());

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(manager
                .verify_must_modify_through_parent(&owned_obj)
                .is_err());
        }

        #[test]
        fn test_inherits_parent_ownership() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);

            let wrapped_obj = create_test_object(Owner::ObjectOwner(parent_id));
            assert!(manager.inherits_parent_ownership(&wrapped_obj));

            let owned_obj = create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64])));
            assert!(!manager.inherits_parent_ownership(&owned_obj));
        }

        #[test]
        fn test_validate_wrapped_object_input() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);
            let wrong_parent_id = ObjectID::new([88u8; 64]);

            let wrapped_obj = create_test_object(Owner::ObjectOwner(parent_id));

            // Should succeed with correct parent
            assert!(manager
                .validate_wrapped_object_input(&wrapped_obj, &parent_id)
                .is_ok());

            // Should fail with wrong parent
            assert!(manager
                .validate_wrapped_object_input(&wrapped_obj, &wrong_parent_id)
                .is_err());
        }

        #[test]
        fn test_unwrap_object() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let wrapped_obj = create_test_object(Owner::ObjectOwner(parent_id));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let unwrapped = manager
                .unwrap_object(&wrapped_obj, new_owner, tx_digest)
                .unwrap();

            assert!(unwrapped.owner.is_address_owned());
            assert_eq!(unwrapped.owner.address(), Some(&new_owner));
            assert_eq!(unwrapped.version.value(), 1);
        }

        #[test]
        fn test_unwrap_object_not_wrapped() {
            let manager = WrappedObjectManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail if not wrapped
            assert!(manager.unwrap_object(&object, new_owner, tx_digest).is_err());
        }

        #[test]
        fn test_filter_wrapped_objects() {
            let manager = WrappedObjectManager::new();
            let parent_id = ObjectID::new([99u8; 64]);

            let objects = vec![
                create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64]))),
                create_test_object(Owner::ObjectOwner(parent_id)),
                create_test_object(Owner::Immutable),
                create_test_object(Owner::ObjectOwner(parent_id)),
            ];

            let wrapped = manager.filter_wrapped_objects(&objects);

            // Should only return the 2 wrapped objects
            assert_eq!(wrapped.len(), 2);
        }

        #[test]
        fn test_filter_by_parent() {
            let manager = WrappedObjectManager::new();
            let parent1 = ObjectID::new([99u8; 64]);
            let parent2 = ObjectID::new([88u8; 64]);

            let objects = vec![
                create_test_object(Owner::AddressOwner(SilverAddress::new([10u8; 64]))),
                create_test_object(Owner::ObjectOwner(parent1)),
                create_test_object(Owner::ObjectOwner(parent2)),
                create_test_object(Owner::ObjectOwner(parent1)),
            ];

            let parent1_children = manager.filter_by_parent(&objects, &parent1);
            assert_eq!(parent1_children.len(), 2);

            let parent2_children = manager.filter_by_parent(&objects, &parent2);
            assert_eq!(parent2_children.len(), 1);
        }
    }

    // Ownership transfer tests
    mod transfer_tests {
        use super::*;

        #[test]
        fn test_transfer_ownership_success() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let (new_object, event) = manager
                .transfer_ownership(&object, &owner, new_owner, tx_digest)
                .unwrap();

            // Verify new object
            assert_eq!(new_object.owner.address(), Some(&new_owner));
            assert_eq!(new_object.version.value(), 1);

            // Verify event
            assert_eq!(event.object_id, object.id);
            assert_eq!(event.old_owner, owner);
            assert_eq!(event.new_owner, new_owner);
            assert_eq!(event.old_version, object.version);
            assert_eq!(event.new_version, new_object.version);
        }

        #[test]
        fn test_transfer_ownership_wrong_owner() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let wrong_owner = SilverAddress::new([30u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail with wrong owner
            assert!(manager
                .transfer_ownership(&object, &wrong_owner, new_owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_transfer_ownership_immutable() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::Immutable);
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail for immutable objects
            assert!(manager
                .transfer_ownership(&object, &owner, new_owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_transfer_ownership_shared() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::Shared {
                initial_shared_version: SequenceNumber::new(0),
            });
            let tx_digest = TransactionDigest::new([1u8; 64]);

            // Should fail for shared objects
            assert!(manager
                .transfer_ownership(&object, &owner, new_owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_batch_transfer_ownership() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let objects = vec![
                create_test_object(Owner::AddressOwner(owner)),
                create_test_object(Owner::AddressOwner(owner)),
                create_test_object(Owner::AddressOwner(owner)),
            ];

            let results = manager
                .batch_transfer_ownership(&objects, &owner, new_owner, tx_digest)
                .unwrap();

            assert_eq!(results.len(), 3);

            // Verify all transfers
            for (new_object, event) in results {
                assert_eq!(new_object.owner.address(), Some(&new_owner));
                assert_eq!(event.new_owner, new_owner);
            }
        }

        #[test]
        fn test_batch_transfer_ownership_partial_failure() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let tx_digest = TransactionDigest::new([1u8; 64]);

            let objects = vec![
                create_test_object(Owner::AddressOwner(owner)),
                create_test_object(Owner::Immutable), // This will fail
                create_test_object(Owner::AddressOwner(owner)),
            ];

            // Should fail because one object is immutable
            assert!(manager
                .batch_transfer_ownership(&objects, &owner, new_owner, tx_digest)
                .is_err());
        }

        #[test]
        fn test_validate_transfer_success() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));

            assert!(manager.validate_transfer(&object, &owner, &new_owner).is_ok());
        }

        #[test]
        fn test_validate_transfer_same_owner() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));

            // Should fail when transferring to same owner
            assert!(manager.validate_transfer(&object, &owner, &owner).is_err());
        }

        #[test]
        fn test_validate_transfer_wrong_current_owner() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let wrong_owner = SilverAddress::new([30u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::AddressOwner(owner));

            // Should fail with wrong current owner
            assert!(manager
                .validate_transfer(&object, &wrong_owner, &new_owner)
                .is_err());
        }

        #[test]
        fn test_validate_transfer_immutable() {
            let manager = OwnershipTransferManager::new();
            let owner = SilverAddress::new([10u8; 64]);
            let new_owner = SilverAddress::new([20u8; 64]);
            let object = create_test_object(Owner::Immutable);

            // Should fail for immutable objects
            assert!(manager.validate_transfer(&object, &owner, &new_owner).is_err());
        }

        #[test]
        fn test_get_transfer_history() {
            let manager = OwnershipTransferManager::new();
            let object_id = ObjectID::new([1u8; 64]);
            let from_owner = SilverAddress::new([1u8; 64]);
            let to_owner = SilverAddress::new([2u8; 64]);

            // Record a transfer
            let transfer = OwnershipTransfer {
                object_id: object_id.clone(),
                from_owner: from_owner.clone(),
                to_owner: to_owner.clone(),
                timestamp: 1000,
                transaction_digest: TransactionDigest::new([0u8; 64]),
            };

            manager.record_transfer(transfer).unwrap();

            // Retrieve transfer history
            let history = manager.get_transfer_history(&object_id);
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].from_owner, from_owner);
            assert_eq!(history[0].to_owner, to_owner);
        }
    }
}
