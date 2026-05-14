import json
import os

data = {
    "item_id": "af28f900-429e-11f1-ac02-3babf827b65c",
    "html_content": "<html><body>Nội dung chi tiết giả lập.</body></html>",
}

os.makedirs("data/output_document_detail", exist_ok=True)
with open("data/output_document_detail/output.jsonl", "w", encoding="utf-8") as f:
    f.write(json.dumps(data, ensure_ascii=False) + "\n")

print("Created fake detail data.")
