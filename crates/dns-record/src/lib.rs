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

use std::fmt::Display;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use chrono::prelude::*;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tracing::debug;

pub mod constants;

/// Wrapper type for time intervals in seconds
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub struct Seconds(pub i32);

impl From<i32> for Seconds {
    fn from(value: i32) -> Self {
        Seconds(value)
    }
}

impl From<Seconds> for i32 {
    fn from(value: Seconds) -> Self {
        value.0
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DnsResourceRecordLookup {
    pub qtype: DnsResourceRecordType,
    pub qname: String,
    pub zone_id: String,
    pub remote: Option<String>,
    pub local: Option<String>,
    #[serde(rename = "real-remote")]
    pub real_remote: Option<String>,
}
#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct DnsResourceRecordReply {
    pub qtype: String,
    pub qname: String,
    pub ttl: u32,
    pub content: DnsResourceRecordContent,
    pub domain_id: Option<String>,
    pub scope_mask: Option<String>,
    pub auth: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DnsResourceRecordContent {
    Address(IpAddr),
    Other(String),
}

impl DnsResourceRecordContent {
    pub fn from_qtype_and_content(
        qtype: DnsResourceRecordType,
        content: impl AsRef<str>,
    ) -> Result<Self, String> {
        let content = content.as_ref();
        match qtype {
            DnsResourceRecordType::A => content
                .parse::<Ipv4Addr>()
                .map(|ip| Self::Address(ip.into()))
                .map_err(|e| format!("Invalid A record content {content}: {e}")),
            DnsResourceRecordType::AAAA => content
                .parse::<Ipv6Addr>()
                .map(|ip| Self::Address(ip.into()))
                .map_err(|e| format!("Invalid AAAA record content {content}: {e}")),
            _ => Ok(Self::Other(content.to_string())),
        }
    }

    pub fn address(&self) -> Option<IpAddr> {
        match self {
            Self::Address(address) => Some(*address),
            Self::Other(_) => None,
        }
    }
}

impl Default for DnsResourceRecordContent {
    fn default() -> Self {
        Self::Other(String::new())
    }
}

impl Display for DnsResourceRecordContent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Address(address) => write!(f, "{address}"),
            Self::Other(content) => write!(f, "{content}"),
        }
    }
}

impl From<IpAddr> for DnsResourceRecordContent {
    fn from(value: IpAddr) -> Self {
        Self::Address(value)
    }
}

impl From<Ipv4Addr> for DnsResourceRecordContent {
    fn from(value: Ipv4Addr) -> Self {
        Self::Address(value.into())
    }
}

impl From<Ipv6Addr> for DnsResourceRecordContent {
    fn from(value: Ipv6Addr) -> Self {
        Self::Address(value.into())
    }
}

impl From<String> for DnsResourceRecordContent {
    fn from(value: String) -> Self {
        Self::Other(value)
    }
}

impl From<&str> for DnsResourceRecordContent {
    fn from(value: &str) -> Self {
        value.to_string().into()
    }
}

impl Serialize for DnsResourceRecordContent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DnsResourceRecordContent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::Other)
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, Eq, PartialEq)]
#[allow(clippy::upper_case_acronyms)]
pub enum DnsResourceRecordType {
    #[default]
    SOA,
    NS,
    A,
    AAAA,
    CNAME,
    MX,
    TXT,
    PTR,
}

impl Display for DnsResourceRecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let record_type = match self {
            DnsResourceRecordType::SOA => constants::DNS_TYPE_SOA,
            DnsResourceRecordType::NS => constants::DNS_TYPE_NS,
            DnsResourceRecordType::A => constants::DNS_TYPE_A,
            DnsResourceRecordType::AAAA => constants::DNS_TYPE_AAAA,
            DnsResourceRecordType::CNAME => constants::DNS_TYPE_CNAME,
            DnsResourceRecordType::MX => constants::DNS_TYPE_MX,
            DnsResourceRecordType::TXT => constants::DNS_TYPE_TXT,
            DnsResourceRecordType::PTR => constants::DNS_TYPE_PTR,
        };
        write!(f, "{record_type}")
    }
}

/// Represents a Start of Authority (SOA) record for a DNS zone.
///
/// The SOA record specifies authoritative information about a DNS zone,
/// including primary nameserver, email contact, and zone update details.
/// It is a critical component in DNS configuration, as it defines zone
/// refresh intervals and update policies.
///
/// # Fields
///
/// * `primary_ns` - The primary nameserver responsible for the zone.
/// * `contact` - The email contact for the zone administrator, typically in the format `hostmaster.example.com`.
/// * `serial` - The serial number for the zone, used to track updates. This should be incremented each time the zone file is modified.
/// * `refresh` - The time (in seconds) a secondary nameserver should wait before querying for zone updates.
/// * `retry` - The time (in seconds) a secondary nameserver should wait before retrying a failed zone update query.
/// * `expire` - The time (in seconds) after which a secondary nameserver should discard the zone if no updates are received.
/// * `minimum` - The minimum TTL (time-to-live) value applied to all resource records in the zone. This specifies how long DNS resolvers should cache data from this zone.
/// * `ttl` - The default TTL (time-to-live) value for the SOA record itself, which is the time period for which DNS clients can cache the SOA record.
///
/// # Example
///
/// ```rust
/// use dns_record::{Seconds, SoaRecord};
/// let soa = SoaRecord {
///     primary_ns: "ns1.example.com".to_string(),
///     contact: "hostmaster.example.com".to_string(),
///     serial: 2024110401,
///     refresh: Seconds(3600),
///     retry: Seconds(600),
///     expire: Seconds(604800),
///     minimum: Seconds(3600),
///     ttl: Seconds(3600),
/// };
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SoaRecord {
    /// The primary nameserver responsible for the DNS zone.
    pub primary_ns: String,
    /// The contact email address of the zone administrator.
    /// Typically formatted as `hostmaster.example.com`.
    pub contact: String,
    /// The serial number for the zone. Increment this number
    /// with each change to the zone to notify secondaries.
    pub serial: u32,
    /// The time interval (in seconds) for a secondary server to refresh the zone.
    pub refresh: Seconds,
    /// The retry interval (in seconds) for a secondary server to retry
    /// if a zone refresh fails.
    pub retry: Seconds,
    /// The expiration time (in seconds) for the zone data on a secondary server.
    /// If no refresh occurs within this time, the zone is considered expired.
    pub expire: Seconds,
    /// The minimum TTL (time-to-live) value for all records in the zone, indicating
    /// how long resolvers should cache records in the absence of specific TTL settings.
    pub minimum: Seconds,
    /// The default TTL (time-to-live) for the SOA record itself.
    pub ttl: Seconds,
}

impl SoaRecord {
    pub fn increment_serial(&mut self) {
        let now = Utc::now();

        // Convert serial to string and strip the last two characters
        let serial_str = self.serial.to_string();
        let stripped_date = &serial_str[..serial_str.len() - 2];

        // Parse the stripped date to check if it's outdated
        let serial_date = stripped_date
            .parse::<u32>()
            .unwrap_or(Self::generate_new_serial());

        let current_date_str = now.format("%Y%m%d").to_string();
        let current_date = current_date_str.parse::<u32>().unwrap_or(0);

        // Check if serial date is outdated
        if serial_date < current_date {
            // Generate a new serial for the new day in `YYYYMMDD01` format
            debug!("DNS serial number is for a different date, generating a new one");
            self.serial = Self::generate_new_serial();
        } else {
            // Increment the last two digits if the date hasn't changed
            let incremented_serial = self.serial + 1;
            debug!("DNS serial number incremented: {}", incremented_serial);
            self.serial = incremented_serial;
        }
    }
    pub fn generate_new_serial() -> u32 {
        let now = Utc::now();
        let formatted_data = now.format("%Y%m%d").to_string() + "01";
        debug!("Serial generated for zone {}", formatted_data);
        formatted_data
            .parse::<u32>()
            .expect("Unable to generate new serial for zone")
    }

    pub fn new(domain_name: &str) -> SoaRecord {
        SoaRecord {
            primary_ns: format!("ns1.{domain_name}"),
            contact: format!("hostmaster.{domain_name}"),
            serial: Self::generate_new_serial(),
            refresh: Seconds(3600),
            retry: Seconds(3600),
            expire: Seconds(604800),
            minimum: Seconds(3600),
            ttl: Seconds(3600),
        }
    }
}

impl Display for SoaRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}. {}. {} {} {} {} {}",
            self.primary_ns,
            self.contact,
            self.serial,
            self.refresh.0,
            self.retry.0,
            self.expire.0,
            self.minimum.0,
        )
    }
}

impl TryFrom<&str> for DnsResourceRecordType {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            constants::DNS_TYPE_SOA => Ok(DnsResourceRecordType::SOA),
            constants::DNS_TYPE_NS => Ok(DnsResourceRecordType::NS),
            constants::DNS_TYPE_A => Ok(DnsResourceRecordType::A),
            constants::DNS_TYPE_AAAA => Ok(DnsResourceRecordType::AAAA),
            constants::DNS_TYPE_CNAME => Ok(DnsResourceRecordType::CNAME),
            constants::DNS_TYPE_MX => Ok(DnsResourceRecordType::MX),
            constants::DNS_TYPE_TXT => Ok(DnsResourceRecordType::TXT),
            constants::DNS_TYPE_PTR => Ok(DnsResourceRecordType::PTR),
            _ => Err(format!("RecordType {value} not implement")),
        }
    }
}

impl TryFrom<String> for DnsResourceRecordType {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.as_str() {
            constants::DNS_TYPE_SOA => Ok(DnsResourceRecordType::SOA),
            constants::DNS_TYPE_NS => Ok(DnsResourceRecordType::NS),
            constants::DNS_TYPE_A => Ok(DnsResourceRecordType::A),
            constants::DNS_TYPE_AAAA => Ok(DnsResourceRecordType::AAAA),
            constants::DNS_TYPE_CNAME => Ok(DnsResourceRecordType::CNAME),
            constants::DNS_TYPE_MX => Ok(DnsResourceRecordType::MX),
            constants::DNS_TYPE_TXT => Ok(DnsResourceRecordType::TXT),
            constants::DNS_TYPE_PTR => Ok(DnsResourceRecordType::PTR),
            _ => Err(format!("RecordType {value} not implement")),
        }
    }
}

impl From<DnsResourceRecordType> for String {
    fn from(value: DnsResourceRecordType) -> Self {
        match value {
            DnsResourceRecordType::SOA => constants::DNS_TYPE_SOA.to_string(),
            DnsResourceRecordType::NS => constants::DNS_TYPE_NS.to_string(),
            DnsResourceRecordType::A => constants::DNS_TYPE_A.to_string(),
            DnsResourceRecordType::AAAA => constants::DNS_TYPE_AAAA.to_string(),
            DnsResourceRecordType::CNAME => constants::DNS_TYPE_CNAME.to_string(),
            DnsResourceRecordType::MX => constants::DNS_TYPE_MX.to_string(),
            DnsResourceRecordType::TXT => constants::DNS_TYPE_TXT.to_string(),
            DnsResourceRecordType::PTR => constants::DNS_TYPE_PTR.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert_json_diff::assert_json_eq;
    use serde_json::json;

    use super::*;

    #[test]
    fn test_dns_resource_record_lookup_as_json() {
        let domain_uuid = uuid::Uuid::new_v4();

        let request = DnsResourceRecordLookup {
            qtype: DnsResourceRecordType::A,
            qname: "foo.example.com".to_string(),
            zone_id: uuid::Uuid::to_string(&domain_uuid),
            remote: None,
            local: None,
            real_remote: None,
        };

        let serialized = serde_json::to_value(&request).unwrap();
        let expected_json = json!({
            "qtype": "A",
            "qname": "foo.example.com",
            "zone_id": domain_uuid.to_string(),
            "remote": null,
            "local": null,
            "real-remote": null,
        });
        assert_json_eq!(serialized, expected_json);
    }

    #[test]
    fn test_dns_resource_record_reply_as_json() {
        let domain_uuid = uuid::Uuid::new_v4();

        let reply = DnsResourceRecordReply {
            qtype: DnsResourceRecordType::A.to_string(),
            qname: "example.com".to_string(),
            ttl: 3600,
            content: DnsResourceRecordContent::from_qtype_and_content(
                DnsResourceRecordType::A,
                "192.168.1.1",
            )
            .unwrap(),
            domain_id: Some(domain_uuid.to_string()),
            scope_mask: None,
            auth: None,
        };

        let serialized_record = serde_json::to_value(&reply).unwrap();

        let expected_json = json!({
            "qtype": "A",
            "qname": "example.com",
            "ttl": 3600,
            "content": "192.168.1.1",
            "domain_id": domain_uuid.to_string(),
            "scope_mask": null,
            "auth": null,
        });

        assert_json_eq!(serialized_record, expected_json);
    }

    #[test]
    fn test_dns_resource_record_content_parses_ip_record_types() {
        let ipv4 =
            DnsResourceRecordContent::from_qtype_and_content(DnsResourceRecordType::A, "192.0.2.1")
                .unwrap();
        assert_eq!(ipv4.address().unwrap().to_string(), "192.0.2.1");

        let ipv6 = DnsResourceRecordContent::from_qtype_and_content(
            DnsResourceRecordType::AAAA,
            "fd00::1",
        )
        .unwrap();
        assert_eq!(ipv6.address().unwrap().to_string(), "fd00::1");

        assert!(
            DnsResourceRecordContent::from_qtype_and_content(DnsResourceRecordType::A, "fd00::1")
                .is_err()
        );

        let soa =
            DnsResourceRecordContent::from_qtype_and_content(DnsResourceRecordType::SOA, "fd00::1")
                .unwrap();
        assert!(matches!(soa, DnsResourceRecordContent::Other(_)));
    }

    #[test]
    fn test_soa_record_dns_lookup_record_reply_as_json() {
        let soa = SoaRecord {
            primary_ns: "ns1.example.com".to_string(),
            contact: "hostmaster.example.com".to_string(),
            serial: 2024110401,
            refresh: Seconds(3600),
            retry: Seconds(600),
            expire: Seconds(604800),
            minimum: Seconds(3600),
            ttl: Seconds(3600),
        };
        let reply = DnsResourceRecordReply {
            qtype: DnsResourceRecordType::SOA.to_string(),
            qname: "example.com".to_string(),
            ttl: 3600,
            content: DnsResourceRecordContent::Other(soa.to_string()),
            domain_id: None,
            scope_mask: None,
            auth: None,
        };

        let serialized = serde_json::to_value(&reply).unwrap();
        let expected_json = json!({
            "qtype": "SOA",
            "qname": "example.com",
            "ttl": 3600,
            "content": "ns1.example.com. hostmaster.example.com. 2024110401 3600 600 604800 3600",
            "domain_id": null,
            "scope_mask": null,
            "auth": null,
        });
        assert_json_eq!(serialized, expected_json);
    }

    #[test]
    fn test_soa_record_as_string() {
        let soa = SoaRecord {
            primary_ns: "ns1.example.com".to_string(),
            contact: "hostmaster.example.com".to_string(),
            serial: 2024110401,
            refresh: Seconds(3600),
            retry: Seconds(600),
            expire: Seconds(604800),
            minimum: Seconds(3600),
            ttl: Seconds(3600),
        };

        let soa_str = soa.to_string();
        assert_eq!(
            soa_str,
            "ns1.example.com. hostmaster.example.com. 2024110401 3600 600 604800 3600"
        );
    }

    #[test]
    fn test_generate_domain_serial_format() {
        // Expected serial format
        let now = Utc::now();
        let expected_serial = now.format("%Y%m%d01").to_string().parse::<u32>().unwrap();

        // Call the function that generates the serial
        let serial = SoaRecord::generate_new_serial();

        assert_eq!(serial, expected_serial);
    }

    #[test]
    fn test_string_to_dns_resource_record_type() {
        assert_eq!(
            DnsResourceRecordType::try_from("SOA".to_string()).unwrap(),
            DnsResourceRecordType::SOA
        );
        assert_eq!(
            DnsResourceRecordType::try_from("NS".to_string()).unwrap(),
            DnsResourceRecordType::NS
        );
        assert_eq!(
            DnsResourceRecordType::try_from("A".to_string()).unwrap(),
            DnsResourceRecordType::A
        );
        assert_eq!(
            DnsResourceRecordType::try_from("AAAA".to_string()).unwrap(),
            DnsResourceRecordType::AAAA
        );
        assert_eq!(
            DnsResourceRecordType::try_from("CNAME".to_string()).unwrap(),
            DnsResourceRecordType::CNAME
        );
        assert_eq!(
            DnsResourceRecordType::try_from("MX".to_string()).unwrap(),
            DnsResourceRecordType::MX
        );
        assert_eq!(
            DnsResourceRecordType::try_from("TXT".to_string()).unwrap(),
            DnsResourceRecordType::TXT
        );
        assert_eq!(
            DnsResourceRecordType::try_from("PTR".to_string()).unwrap(),
            DnsResourceRecordType::PTR
        );
    }

    #[test]
    fn test_dns_resource_record_type_to_string() {
        assert_eq!(String::from(DnsResourceRecordType::SOA), "SOA".to_string());
        assert_eq!(String::from(DnsResourceRecordType::NS), "NS".to_string());
        assert_eq!(String::from(DnsResourceRecordType::A), "A".to_string());
        assert_eq!(
            String::from(DnsResourceRecordType::AAAA),
            "AAAA".to_string()
        );
        assert_eq!(
            String::from(DnsResourceRecordType::CNAME),
            "CNAME".to_string()
        );
        assert_eq!(String::from(DnsResourceRecordType::MX), "MX".to_string());
        assert_eq!(String::from(DnsResourceRecordType::TXT), "TXT".to_string());
        assert_eq!(String::from(DnsResourceRecordType::PTR), "PTR".to_string());
    }

    #[test]
    fn test_string_to_dns_resource_record_type_unimplemented() {
        assert_eq!(
            DnsResourceRecordType::try_from("FAKE".to_string()),
            Err("RecordType FAKE not implement".to_string())
        )
    }
}
