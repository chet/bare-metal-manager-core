/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

use ::rpc::errors::RpcDataConversionError;
use carbide_uuid::rack::RackId;
use serde::Deserialize;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};

use crate::metadata::{Metadata, default_metadata_for_deserializer};

/// ExpectedRack represents a rack that has been declared and is expected to
/// be fully populated with compute trays, switches, and power shelves. The
/// rack_type references a RackCapabilitiesSet in the config file which
/// defines the expected device counts.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExpectedRack {
    /// rack_id is the rack identifier, which comes from the DCIM.
    pub rack_id: RackId,

    #[serde(flatten)]
    pub data: ExpectedRackData,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ExpectedRackData {
    /// rack_type is the type of rack (e.g. "NVL72") that maps to a
    /// RackCapabilitiesSet in the config file, defining expected device counts.
    pub rack_type: String,

    /// User-defined metadata for the rack.
    #[serde(default = "default_metadata_for_deserializer")]
    pub metadata: Metadata,
}

impl<'r> FromRow<'r, PgRow> for ExpectedRack {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let labels: sqlx::types::Json<HashMap<String, String>> = row.try_get("metadata_labels")?;
        let metadata = Metadata {
            name: row.try_get("metadata_name")?,
            description: row.try_get("metadata_description")?,
            labels: labels.0,
        };

        Ok(ExpectedRack {
            rack_id: row.try_get("rack_id")?,
            data: ExpectedRackData {
                rack_type: row.try_get("rack_type")?,
                metadata,
            },
        })
    }
}

impl From<ExpectedRack> for rpc::forge::ExpectedRack {
    fn from(expected_rack: ExpectedRack) -> Self {
        rpc::forge::ExpectedRack {
            rack_id: Some(expected_rack.rack_id),
            rack_type: expected_rack.data.rack_type,
            metadata: Some(expected_rack.data.metadata.into()),
        }
    }
}

impl TryFrom<rpc::forge::ExpectedRack> for ExpectedRack {
    type Error = RpcDataConversionError;

    fn try_from(rpc: rpc::forge::ExpectedRack) -> Result<Self, Self::Error> {
        let rack_id = rpc
            .rack_id
            .ok_or(RpcDataConversionError::MissingArgument("rack_id"))?;
        if rpc.rack_type.is_empty() {
            return Err(RpcDataConversionError::InvalidArgument(
                "rack_type is required".to_string(),
            ));
        }
        let metadata = Metadata::try_from(rpc.metadata.unwrap_or_default())?;

        Ok(ExpectedRack {
            rack_id,
            data: ExpectedRackData {
                rack_type: rpc.rack_type,
                metadata,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that serde(flatten) correctly deserializes a flat JSON
    /// into the envelope + ExpectedRackData structure.
    #[test]
    fn deserialize_flat_json_into_envelope_and_data() {
        let json = r#"{
            "rack_id": "rack-42",
            "rack_type": "NVL72",
            "metadata": {
                "name": "My Rack",
                "description": "Production rack",
                "labels": {"env": "prod"}
            }
        }"#;

        let rack: ExpectedRack = serde_json::from_str(json).unwrap();

        // Envelope field
        assert_eq!(rack.rack_id.to_string(), "rack-42");

        // Data fields (via flatten)
        assert_eq!(rack.data.rack_type, "NVL72");
        assert_eq!(rack.data.metadata.name, "My Rack");
        assert_eq!(rack.data.metadata.description, "Production rack");
        assert_eq!(
            rack.data.metadata.labels.get("env"),
            Some(&"prod".to_string())
        );
    }

    /// Verify that optional/defaulted fields deserialize correctly
    /// when omitted from the JSON input.
    #[test]
    fn deserialize_minimal_json_uses_defaults() {
        let json = r#"{
            "rack_id": "rack-99",
            "rack_type": "Single"
        }"#;

        let rack: ExpectedRack = serde_json::from_str(json).unwrap();

        assert_eq!(rack.data.rack_type, "Single");
        assert!(rack.data.metadata.name.is_empty());
        assert!(rack.data.metadata.labels.is_empty());
    }
}
