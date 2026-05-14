import json
import os

data = {
    "id": "af28f900-429e-11f1-ac02-3babf827b65c",
    "item_id": "af28f900-429e-11f1-ac02-3babf827b65c",
    "title": "Quyết định 1628/QĐ-UBND Bãi bỏ toàn bộ Quyết định số 33/2022/QĐ-UBND ngày 08 tháng 8 năm 2022...",
    "docAbs": None,
    "docNum": "1628/QĐ-UBND",
    "docType": {
        "id": "0a5362e8-cdca-436e-96cd-979598df3b16",
        "name": "Quyết định",
        "code": "QĐ",
    },
    "issueDate": "2026-04-24T00:00:00",
    "effFrom": "2026-04-24T00:00:00",
    "effTo": None,
    "publicDate": None,
    "updatedDate": "2026-04-28T10:29:38.593859",
    "effStatus": {
        "id": "1419f6be-4a15-44a7-97ac-ea042770a514",
        "code": "CHL",
        "name": "Còn hiệu lực",
    },
    "documentMajors": [
        {
            "id": "df8c1420-42b1-11f1-be38-214bba468a5a",
            "majorType": {
                "code": "nganh_442",
                "name": "Nông nghiệp và Môi trường",
                "nameEn": None,
                "shortName": None,
            },
            "fieldType": None,
        }
    ],
    "isNew": True,
    "documentRelatedList": [
        {
            "id": "af3506f0-429e-11f1-9d65-23e6cf41894d",
            "fileName": "1628_QD_QD_2026.pdf",
            "relatedType": "1",
            "fileTitle": None,
            "fileOrder": None,
        }
    ],
    "sourceDocumentId": None,
    "isLw": False,
}

os.makedirs("data/output_document_list", exist_ok=True)
with open("data/output_document_list/output.jsonl", "w", encoding="utf-8") as f:
    f.write(json.dumps(data, ensure_ascii=False) + "\n")

print("Created fake data matching a1.py structure.")
