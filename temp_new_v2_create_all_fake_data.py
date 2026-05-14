# import json
# import os

# # Create fake document_total data
# total_data = {
#     "update_at": "2026-05-14T22:03:00.000000",
#     "total_count": 167842
# }

# os.makedirs("data/output_document_total", exist_ok=True)
# with open("data/output_document_total/output.jsonl", "w", encoding="utf-8") as f:
#     f.write(json.dumps(total_data, ensure_ascii=False) + "\n")

# print("✅ Created fake total_count data")

# # Create fake document_list data
# list_data = [
#     {
#         "id": "17177bb0-4f7c-11f1-a49e-8124028d376e",
#         "item_id": "17177bb0-4f7c-11f1-a49e-8124028d376e",
#         "title": "Quy định về hoạt động môi giới tiền tệ của ngân hàng thương mại",
#         "docAbs": None,
#         "docNum": "43/VBHN-NHNN",
#         "docType": {
#             "id": "26b8a9ff-1b59-4c57-9605-f2ad4ed7c324",
#             "name": "Văn bản hợp nhất",
#             "code": "VBHN"
#         },
#         "issueDate": "2026-05-11T00:00:00",
#         "effFrom": None,
#         "effTo": None,
#         "publicDate": None,
#         "updatedDate": "2026-05-14T17:22:52.331853",
#         "effStatus": None,
#         "documentMajors": [],
#         "isNew": True,
#         "documentRelatedList": None,
#         "sourceDocumentId": None,
#         "isLw": True,
#         "update_at": "2026-05-14T22:03:00.000000"
#     },
#     {
#         "id": "aeec5ca0-4ebc-11f1-ae9f-0d02eb8dfae1",
#         "item_id": "aeec5ca0-4ebc-11f1-ae9f-0d02eb8dfae1",
#         "title": "Thông tư quy định về phân cấp thẩm quyền chứng nhận lãnh sự",
#         "docAbs": None,
#         "docNum": "03/2026/TT-BNG",
#         "docType": {
#             "id": "178c63a9-73ff-4fd4-9d91-18d690520090",
#             "name": "Thông tư",
#             "code": "TT"
#         },
#         "issueDate": "2026-05-06T00:00:00",
#         "effFrom": "2026-05-09T00:00:00",
#         "effTo": None,
#         "publicDate": None,
#         "updatedDate": "2026-05-13T18:16:30.522669",
#         "effStatus": {
#             "id": "1419f6be-4a15-44a7-97ac-ea042770a514",
#             "code": "CHL",
#             "name": "Còn hiệu lực"
#         },
#         "documentMajors": [
#             {
#                 "id": "aeed9520-4ebc-11f1-9895-d14ca5b346df",
#                 "majorType": {
#                     "code": "nganh_55",
#                     "name": "Ngoại giao",
#                     "nameEn": None,
#                     "shortName": None
#                 },
#                 "fieldType": None
#             }
#         ],
#         "isNew": True,
#         "documentRelatedList": None,
#         "sourceDocumentId": None,
#         "isLw": True,
#         "update_at": "2026-05-14T22:03:00.000000"
#     }
# ]

# os.makedirs("data/output_document_list", exist_ok=True)
# with open("data/output_document_list/output.jsonl", "w", encoding="utf-8") as f:
#     for item in list_data:
#         f.write(json.dumps(item, ensure_ascii=False) + "\n")

# print("✅ Created fake document_list data (2 items)")

# # Create fake document_detail data
# detail_data = [
#     {
#         "item_id": "17177bb0-4f7c-11f1-a49e-8124028d376e",
#         "html_content": "<html><body><h1>Quy định về hoạt động môi giới tiền tệ</h1><p>Nội dung chi tiết văn bản...</p></body></html>",
#         "file_url": "https://example.com/doc1.html"
#     },
#     {
#         "item_id": "aeec5ca0-4ebc-11f1-ae9f-0d02eb8dfae1",
#         "html_content": "<html><body><h1>Thông tư về phân cấp thẩm quyền</h1><p>Nội dung chi tiết...</p></body></html>",
#         "file_url": "https://example.com/doc2.html"
#     }
# ]

# os.makedirs("data/output_document_detail", exist_ok=True)
# with open("data/output_document_detail/output.jsonl", "w", encoding="utf-8") as f:
#     for item in detail_data:
#         f.write(json.dumps(item, ensure_ascii=False) + "\n")

# print("✅ Created fake document_detail data (2 items)")
