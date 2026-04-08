import json
import os
import re
from typing import Dict, List, Optional, Any
import pandas as pd

class HL7Message:
    """
    Advanced HL7 V2.x Message Engine for Python.
    Provides high-performance parsing with Universal Caret Intelligence.
    """
    
    def __init__(self, data: str, version: Optional[str] = None):
        self.raw_data = data
        self.lines = [line.strip() for line in data.split("\n") if line.strip()]
        
        # Load schema
        schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        with open(schema_path, "r") as f:
            self._schema = json.load(f)

        # Automatic Version Detection
        if version:
            self.version = version
        else:
            self.version = self._detect_version() or "2.3"
            
    def _detect_version(self) -> Optional[str]:
        """Detect HL7 version from MSH-12."""
        msh_lines = self.get_segment_lines("MSH")
        if not msh_lines:
            return None
        
        fields = msh_lines[0].split("|")
        # For MSH, index 12 is fields[11] because fields[0]="MSH" and fields[1]="^~\\&"
        if len(fields) >= 12:
            return fields[11]
        return None

    def get_segment_lines(self, segment_type: str) -> List[str]:
        """Extract all lines for a specific segment type."""
        prefix = f"{segment_type}|"
        return [line for line in self.lines if line.startswith(prefix)]

    def _apply_caret_intelligence(self, field_name: str, val: str, result: Dict[str, Any]):
        """Universal Caret Intelligence (Smart Splitter)."""
        if "^" not in val:
            return
            
        parts = val.split("^")
        
        # Name/Provider Logic
        if any(keyword in field_name for keyword in ["NAME", "DOCTOR", "PROVIDER", "PERSON", "STAFF"]):
            if len(parts) >= 1: result[f"{field_name}_LAST"] = parts[0]
            if len(parts) >= 2: result[f"{field_name}_FIRST"] = parts[1]
            if field_name == "PATIENT_NAME":
                result["LAST_NAME"] = parts[0]
                result["FIRST_NAME"] = parts[1]
                
        # ID/Code Logic
        elif any(keyword in field_name for keyword in ["CODE", "ID", "STATUS", "TYPE"]):
            if len(parts) >= 1: result[f"{field_name}_ID"] = parts[0]
            if len(parts) >= 2: result[f"{field_name}_TEXT"] = parts[1]
            
        # Address/Location Logic
        elif any(keyword in field_name for keyword in ["ADDRESS", "LOCATION", "POINT_OF_CARE"]):
            if len(parts) >= 1: result[f"{field_name}_STREET"] = parts[0]
            if len(parts) >= 2: result[f"{field_name}_ROOM"] = parts[1]
            if len(parts) >= 3: result[f"{field_name}_CITY"] = parts[2]

    def parse_segment(self, segment_type: str) -> Optional[pd.DataFrame]:
        """Parse all instances of a segment into a Pandas DataFrame."""
        lines = self.get_segment_lines(segment_type)
        if not lines:
            return None
            
        # Get mapping for version
        ver_schema = self._schema.get(self.version, self._schema.get("2.3"))
        mapping = ver_schema.get(segment_type)
        
        if not mapping:
            return None
            
        results = []
        for line in lines:
            fields = line.split("|")
            result = {}
            
            for field_name, idx in mapping.items():
                # Special MSH logic (MSH-1 is separator, MSH-2 is encoding)
                if segment_type == "MSH":
                    if idx == 1:
                        val = "|"
                    elif idx <= len(fields):
                        val = fields[idx-1] # MSH is special: fields[0]="MSH", fields[1]="^~\\&"
                    else:
                        val = ""
                else:
                    # fields[0] is segment header, so index 1 is fields[1]
                    val = fields[idx] if idx < len(fields) else ""
                
                if val:
                    result[field_name] = val
                    self._apply_caret_intelligence(field_name, val, result)
                    
            results.append(result)
            
        return pd.DataFrame(results)

    def write_segment(self, segment_type: str, data: Dict[str, Any]) -> str:
        """Write a segment from a dictionary of fields."""
        ver_schema = self._schema.get(self.version, self._schema.get("2.3"))
        mapping = ver_schema.get(segment_type)
        
        if not mapping:
            raise ValueError(f"No schema mapping found for segment {segment_type} in version {self.version}")
            
        max_idx = max(mapping.values())
        
        # Special MSH Handling
        if segment_type == "MSH":
            fields = [""] * max_idx
            fields[0] = "MSH"
            fields[1] = "^~\\&"
            for field_name, idx in mapping.items():
                if idx > 2 and field_name in data:
                    fields[idx-1] = str(data[field_name])
            return "|".join(fields[:2]) + "|" + "|".join(fields[2:])

        fields = [""] * (max_idx + 1)
        fields[0] = segment_type
        for field_name, idx in mapping.items():
            if field_name in data:
                fields[idx] = str(data[field_name])
        return "|".join(fields)

    @property
    def clinical_summary(self) -> pd.DataFrame:
        """
        Mimics rHL7's extract_HL7 function.
        Returns a single-row DataFrame with standardized clinical fields.
        """
        # Get DataFrames for key segments
        msh = self.parse_segment("MSH")
        pid = self.parse_segment("PID")
        pv1 = self.parse_segment("PV1")
        
        # Helper to get first value or None
        def get_val(df, field, index=0):
            if df is not None and not df.empty and field in df.columns:
                val = df.iloc[index][field]
                return val if val else None
            return None

        summary = {
            "MANDT": get_val(msh, "SENDING_FACILITY"),
            "PATID": get_val(pid, "EXTERNAL_PATIENT_ID"),
            "PATNR": get_val(pid, "INTERNAL_PATIENT_ID"),
            "FALNR": get_val(pv1, "VISIT_NUMBER"),
            "VORNAME": get_val(pid, "PATIENT_NAME_FIRST") or get_val(pid, "FIRST_NAME"),
            "NACHNAME": get_val(pid, "PATIENT_NAME_LAST") or get_val(pid, "LAST_NAME"),
            "GEBDAT": get_val(pid, "DOB"),
            "GSCHL": get_val(pid, "GENDER"),
            "KLASSE": get_val(pv1, "PATIENT_TYPE"),
            "BEWTY": get_val(pv1, "ADMISSION_TYPE"),
            "ORGPF": get_val(pv1, "POINT_OF_CARE_STREET") or get_val(pv1, "POINT_OF_CARE"),
            "DATUM": get_val(msh, "DATE_TIME"),
        }
        return pd.DataFrame([summary])

    def __getattr__(self, name):
        """Dynamic access to segments via properties (e.g. message.pid, message.obr)."""
        segment_type = name.upper()
        # Only handle 3-letter segment types
        if len(segment_type) == 3 and segment_type.isalnum():
            return self.parse_segment(segment_type)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")




    def create_ack(self, ack_code: str = "AA", text_message: str = "") -> str:
        """
        Enterprise 'ACK Swap': Automatically generate a standard HL7 Acknowledgment.
        Inverts Sending/Receiving applications and preserves the Message Control ID.
        """
        # Determine separators
        msh_lines = self.get_segment_lines("MSH")
        if not msh_lines:
            raise ValueError("Cannot create ACK: Message has no MSH segment.")
        
        msh_orig = msh_lines[0].split("|")
        field_sep = "|"
        enc_chars = msh_orig[1] if len(msh_orig) > 1 else "^~\\&"
        
        # Extract routing info for the swap
        # MSH-3 (Sending App), MSH-4 (Sending Fac), MSH-5 (Rec App), MSH-6 (Rec Fac)
        snd_app = msh_orig[2] if len(msh_orig) > 2 else ""
        snd_fac = msh_orig[3] if len(msh_orig) > 3 else ""
        rec_app = msh_orig[4] if len(msh_orig) > 4 else ""
        rec_fac = msh_orig[5] if len(msh_orig) > 5 else ""
        
        msg_control_id = msh_orig[9] if len(msh_orig) > 9 else ""
        version = self.version
        
        from datetime import datetime
        dt_str = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Build new MSH (Swap 3<->5 and 4<->6)
        ack_msh = [
            "MSH", enc_chars, rec_app, rec_fac, snd_app, snd_fac, dt_str, "",
            "ACK", msg_control_id, "P", version
        ]
        
        # Build MSA
        ack_msa = ["MSA", ack_code, msg_control_id, text_message]
        
        return field_sep.join(ack_msh) + "\n" + field_sep.join(ack_msa)

def parse(raw_data: str, version: Optional[str] = None) -> Any: # Returns Union[HL7Message, List[HL7Message]]
    """
    Enterprise entry point. Detects BHS/FHS envelopes and strips them to yield
    a list of HL7Messages, or returns a single HL7Message if no batch headers found.
    """
    raw_data = raw_data.strip()
    if not raw_data:
        return HL7Message("")
        
    lines = [line.strip() for line in raw_data.split("\n") if line.strip()]
    
    # Check for Batch/File Headers
    if lines[0].startswith("FHS|") or lines[0].startswith("BHS|"):
        messages = []
        current_msg_lines = []
        
        for line in lines:
            # Skip header/trailer envelopes
            if line.startswith("FHS|") or line.startswith("FTS|") or \
               line.startswith("BHS|") or line.startswith("BTS|"):
                continue
                
            if line.startswith("MSH|"):
                # If we already have lines, compile previous message
                if current_msg_lines:
                    messages.append(HL7Message("\n".join(current_msg_lines), version))
                    current_msg_lines = []
            
            # If we hit segments before an MSH in a batch, it's malformed, but we capture anyway
            current_msg_lines.append(line)
            
        # Add the final trailing message
        if current_msg_lines:
            messages.append(HL7Message("\n".join(current_msg_lines), version))
            
        return messages
        
    # Standard single message parsing
    return HL7Message(raw_data, version)

def read_hl7(file_path: str, version: Optional[str] = None) -> Any:
    """Read an HL7 file. Wraps the enterprise batch-aware parser."""
    with open(file_path, "r") as f:
        data = f.read()
    return parse(data, version)

