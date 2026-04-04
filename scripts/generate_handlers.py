import json
import os
import textwrap

def generate_segment_handlers(schema_path: str, output_dir: str):
    """
    Python segment handler generator.
    Produces 113+ segment modules from the JSON schema.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    with open(schema_path, "r") as f:
        schema = json.load(f)
        
    # We use 2.3 as the base for generating the list of unique segments
    base_version = schema["2.3"]
    segments = sorted(base_version.keys())
    
    for segment in segments:
        file_name = f"{segment.lower()}.py"
        file_path = os.path.join(output_dir, file_name)
        
        content = textwrap.dedent(f'''\
            """
            pyHL7 {segment} Segment Handler
            Automatically generated from HL7 v2.x schema.
            """
            from typing import Optional
            import pandas as pd
            from ..core import HL7Message

            def get_{segment.lower()}(message: HL7Message, version: str = "2.3") -> Optional[pd.DataFrame]:
                """Extract and parse the {segment} segment from the HL7 message."""
                return message.parse_segment("{segment}")
                
            def write_{segment.lower()}(data: pd.DataFrame, version: str = "2.3") -> str:
                """Write data to HL7 {segment} segment format (To be implemented)."""
                # Placeholder for bi-directional support
                pass
        ''')
        
        with open(file_path, "w") as f:
            f.write(content)
            
    # Generate __init__.py to export all segments
    init_path = os.path.join(output_dir, "__init__.py")
    imports = [f"from .{seg.lower()} import get_{seg.lower()}" for seg in segments]
    
    with open(init_path, "w") as f:
        f.write("# pyHL7 Segment Handlers\n")
        f.write("\n".join(imports))
        f.write("\n")

if __name__ == "__main__":
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    schema_file = os.path.join(base_dir, "src", "pyhl7", "schema.json")
    segments_dir = os.path.join(base_dir, "src", "pyhl7", "segments")
    
    generate_segment_handlers(schema_file, segments_dir)
    print(f"Generated handlers for all segments in {{segments_dir}}")
