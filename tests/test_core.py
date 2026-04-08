import pytest
import pandas as pd
from pyhl7 import HL7Message

# Sample HL7 v2.3 Message
HL7_V23 = """MSH|^~\\&|SENDING_FAC|FACILITY|RECEIVING_APP|RECEIVING_FAC|202310271230||ADT^A01|123456|P|2.3
PID|1|EXT1234567|INT1234567||DOE^JOHN^MIDDLE||19800101|M||RACE|123 STREET^^CITY^^ZIP||||MARRIED||ACCOUNT123
PV1|1|I|POINT^ROOM^BED^FACILITY|R|||ATTENDING^DR|||SERVICE|||||||ADMITTING^DR|V123456"""

# Sample HL7 v2.5 Message (Notice MSH-12 is 2.5)
HL7_V25 = """MSH|^~\\&|SENDING_FAC|FACILITY|RECEIVING_APP|RECEIVING_FAC|202310271230||ADT^A01|654321|P|2.5
PID|1|EXT7654321|INT7654321||SMITH^JANE||19900505|F||RACE|456 AVENUE^^TOWN^^ZIP||||SINGLE||ACCOUNT456"""

def test_version_detection():
    msg23 = HL7Message(HL7_V23)
    assert msg23.version == "2.3"
    
    msg25 = HL7Message(HL7_V25)
    assert msg25.version == "2.5"

def test_dynamic_segment_access():
    msg = HL7Message(HL7_V23)
    # Test PID (lowercase property-style)
    pid = msg.pid
    assert isinstance(pid, pd.DataFrame)
    assert pid.iloc[0]["EXTERNAL_PATIENT_ID"] == "EXT1234567"
    
    # Test MSH
    msh = msg.msh
    assert msh.iloc[0]["SENDING_FACILITY"] == "FACILITY"
    
    # Test non-existent segment
    assert msg.abc is None

def test_universal_caret_intelligence():
    msg = HL7Message(HL7_V23)
    pid = msg.pid
    # Name splitting
    assert pid.iloc[0]["PATIENT_NAME_LAST"] == "DOE"
    assert pid.iloc[0]["PATIENT_NAME_FIRST"] == "JOHN"
    assert pid.iloc[0]["LAST_NAME"] == "DOE" # Special case for PATIENT_NAME
    
    # Address splitting (PV1-3 Point of Care)
    pv1 = msg.pv1
    assert pv1.iloc[0]["PATIENT_LOCATION_STREET"] == "POINT"
    assert pv1.iloc[0]["PATIENT_LOCATION_ROOM"] == "ROOM"

def test_clinical_summary():
    msg = HL7Message(HL7_V23)
    summary = msg.clinical_summary
    assert isinstance(summary, pd.DataFrame)
    assert summary.iloc[0]["PATID"] == "EXT1234567"
    assert summary.iloc[0]["VORNAME"] == "JOHN"
    assert summary.iloc[0]["NACHNAME"] == "DOE"
    assert summary.iloc[0]["MANDT"] == "FACILITY"

def test_write_segment():
    msg = HL7Message("", version="2.3")
    
    # Write MSH
    msh_str = msg.write_segment("MSH", {
        "SENDING_APPLICATION": "TEST_APP",
        "MESSAGE_CONTROL_ID": "999"
    })
    assert msh_str.startswith("MSH|^~\\&|TEST_APP")
    assert "999" in msh_str
    
    # Write PID
    pid_str = msg.write_segment("PID", {
        "SET_ID": "1",
        "EXTERNAL_PATIENT_ID": "EXT001"
    })
    assert pid_str.startswith("PID|1|EXT001")

def test_attribute_error():
    msg = HL7Message(HL7_V23)
    with pytest.raises(AttributeError):
        msg.not_a_segment_type_long_name

def test_ack_generation():
    msg = HL7Message(HL7_V23)
    ack_str = msg.create_ack("AA", "Message Processed Successfully")
    
    msh_part = ack_str.split("\n")[0]
    msa_part = ack_str.split("\n")[1]
    
    # Assert ACK Swap occurred
    msh_fields = msh_part.split("|")
    assert msh_fields[2] == "RECEIVING_APP"  # Sending App is now the original Receiver
    assert msh_fields[4] == "SENDING_FAC"    # Receiving App is now the original Sender
    assert msh_fields[8] == "ACK"            # Message Type changed to ACK
    assert msh_fields[9] == "123456"         # Preserved Control ID
    
    # Assert MSA is linked correctly
    msa_fields = msa_part.split("|")
    assert msa_fields[0] == "MSA"
    assert msa_fields[1] == "AA"
    assert msa_fields[2] == "123456"         # Link to original Control ID
    assert msa_fields[3] == "Message Processed Successfully"

def test_batch_parsing():
    batch_data = f"BHS|^~\\&|SENDER|RECEIVER|20231027\n{HL7_V23}\n{HL7_V25}\nBTS|2"
    from pyhl7 import parse
    
    messages = parse(batch_data)
    assert isinstance(messages, list)
    assert len(messages) == 2
    assert messages[0].version == "2.3"
    assert messages[1].version == "2.5"
    assert messages[0].msh.iloc[0]["MESSAGE_CONTROL_ID"] == "123456"
