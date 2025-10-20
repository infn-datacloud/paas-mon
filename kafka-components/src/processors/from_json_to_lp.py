from pathlib import Path
from modules.from_json_to_lp.lp_builder import json_to_lines
import json
if __name__ == "__main__":
    repo_root = Path(__file__).resolve().parents[3]
    test_dir = repo_root / "test" / "influxdb-processor"
    input_json = test_dir / "input" / "example_message.json"
    output_lp  = test_dir / "output" / "converted_file.lp"

    try:
     with open(input_json) as f:
        msg = json.load(f)  
    except Exception as e:
        print("[ERROR]", e)
    else:
        lines = json_to_lines(msg)
        body = "\n".join(lines)

        with open(output_lp, "w") as f:
            f.write(body)

