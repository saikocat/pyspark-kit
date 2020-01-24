from typing import Dict, Any

from deepdiff import DeepDiff


def find_diff(prev_schema: Any, curr_schema: Any) -> Dict[str, Any]:
    ddiff = DeepDiff(prev_schema, curr_schema, ignore_order=False)
    return ddiff.to_dict()
