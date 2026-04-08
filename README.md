# pyhl7: Advanced HL7 V2.x Message Engine for Python

![pyhl7 Logo](https://raw.githubusercontent.com/saiveeranki/pyhl7/master/logo.png)

`pyhl7` is a high-performance, version-aware HL7 v2.x parsing and data engineering engine for Python. Ported from the robust `rHL7` R package, it brings "Universal Caret Intelligence" and schema-driven segment handling to clinical data pipelines.

## Key Features

- **Version Awareness**: Automatically detects HL7 versions (e.g., 2.3, 2.5) via `MSH-12` and applies the correct schema definitions.
- **Universal Caret Intelligence**: Automatically splits complex fields (Names, Addresses, Identifiers) into intuitive sub-components (e.g., `PATIENT_NAME_LAST`, `PATIENT_NAME_FIRST`).
- **Dynamic Segment Properties**: Access any of the 100+ standard HL7 segments as a property (e.g., `msg.pid`, `msg.obr`, `msg.pv1`).
- **Clinical Data Engineering**: Extract standardized medical records into flat [Pandas](https://pandas.pydata.org/) DataFrames with the `clinical_summary` property.
- **Segment Writing**: Generate HL7-compliant strings from dictionary data using `write_segment`.
- **Enterprise Batch Parsing**: Safely handle massive clinical data feeds with native support for stripping `FHS/BHS` (File/Batch Header) envelopes.
- **Native ACK Generation**: Perform an automated "ACK Swap" to instantly generate standard `MSA` acknowledgments with synchronized routing IDs.

## Installation

```bash
# Standard installation
pip install -r requirements.txt
pip install -e .

# Development installation (includes tests and formatting)
pip install -r requirements-dev.txt
```

## Quick Start

### Basic Parsing

```python
from pyhl7 import HL7Message

# Raw HL7 v2.3 Message
raw_hl7 = """MSH|^~\\&|SENDING_FAC|FACILITY|RECEIVING_APP|RECEIVING_FAC|202310271230||ADT^A01|123456|P|2.3
PID|1|EXT1234567|INT1234567||DOE^JOHN^MIDDLE||19800101|M||RACE|123 STREET^^CITY^^ZIP||||MARRIED||ACCOUNT123"""

msg = HL7Message(raw_hl7)

# Access segments dynamically
pid_df = msg.pid
print(pid_df[["EXTERNAL_PATIENT_ID", "PATIENT_NAME_LAST", "PATIENT_NAME_FIRST"]])

# Use Clinical Summary for rapid extraction
summary = msg.clinical_summary
print(summary[["FALNR", "PATID", "VORNAME", "NACHNAME"]])
```

### Writing Segments

```python
msg = HL7Message("", version="2.3")
new_msh = msg.write_segment("MSH", {
    "SENDING_APPLICATION": "MY_APP",
    "MESSAGE_CONTROL_ID": "ABC-123"
})
print(new_msh)
# MSH|^~\&|MY_APP|||||||ABC-123|||||||
```

## Advanced Usage

### Enterprise Batch Processing
Hospital feeds often bundle thousands of messages inside single files using Batch or File headers. Use the top-level `parse()` method to safely strip out these outer envelopes and return a clean list of `HL7Message` objects:

```python
from pyhl7 import parse

raw_batch = "BHS|^~\\&|SENDER|RECEIVER|...\nMSH|...\nPID|...\nBTS|1"
messages = parse(raw_batch) # Returns a List[HL7Message] if BHS/FHS is detected.
```

### Automated ACK Swap
If you are building an integration endpoint, you need to acknowledge inbound messages. The `create_ack()` method automatically flips the Sending/Receiving applications and preserves the `MESSAGE_CONTROL_ID` for perfect synchronization:

```python
ack_string = msg.create_ack(ack_code="AA", text_message="Successfully injected into LIS.")
print(ack_string)
# MSH|...|RECEIVER|...|SENDER|...
# MSA|AA|ORIGINAL_CONTROL_ID|Successfully injected into LIS.
```

### Schema Customization
`pyhl7` uses a centralized `schema.json` to map HL7 field names to indices. This ensures consistency across different HL7 versions and segments.

### Universal Caret Intelligence
When a field contains caret delimiters (`^`), `pyhl7` automatically expands it into multiple named fields based on the field name context (e.g., `NAME`, `ADDRESS`, `CODE`).

## License
Apache License 2.0. Developed by Sai Veeranki.
