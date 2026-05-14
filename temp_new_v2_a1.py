# import requests

# url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"
# payload = {"pageSize": 2, "pageNumber": 1}
# response = requests.request("POST", url, json=payload)
# print(response.text)


# {
#     "success": true,
#     "statusCode": 200,
#     "message": "OK",
#     "messageCode": "ok",
#     "debugMessage": "",
#     "data": {
#         "current": 1,
#         "total": 166921,
#         "pageNumber": 0,
#         "pageSize": 2,
#         "items": [
#             {
#                 "id": "af28f900-429e-11f1-ac02-3babf827b65c",
#                 "title": "Quyết định 1628/QĐ-UBND Bãi bỏ toàn bộ Quyết định số 33/2022/QĐ-UBND ngày 08 tháng 8 năm 2022 của Ủy ban nhân dân tỉnh Đồng Nai ban hành quy định về thẩm quyền thẩm định, phê duyệt Kế hoạch ứng phó sự cố tràn dầu cấp cơ sở trên địa bàn tỉnh Đồng Nai",
#                 "docAbs": null,
#                 "docNum": "1628/QĐ-UBND",
#                 "docType": {
#                     "id": "0a5362e8-cdca-436e-96cd-979598df3b16",
#                     "name": "Quyết định",
#                     "code": "QĐ",
#                 },
#                 "issueDate": "2026-04-24T00:00:00",
#                 "effFrom": "2026-04-24T00:00:00",
#                 "effTo": null,
#                 "publicDate": null,
#                 "updatedDate": "2026-04-28T10:29:38.593859",
#                 "effStatus": {
#                     "id": "1419f6be-4a15-44a7-97ac-ea042770a514",
#                     "code": "CHL",
#                     "name": "Còn hiệu lực",
#                 },
#                 "documentMajors": [
#                     {
#                         "id": "df8c1420-42b1-11f1-be38-214bba468a5a",
#                         "majorType": {
#                             "code": "nganh_442",
#                             "name": "Nông nghiệp và Môi trường",
#                             "nameEn": null,
#                             "shortName": null,
#                         },
#                         "fieldType": null,
#                     }
#                 ],
#                 "isNew": true,
#                 "documentRelatedList": [
#                     {
#                         "id": "af3506f0-429e-11f1-9d65-23e6cf41894d",
#                         "fileName": "1628_QD_QD_2026.pdf",
#                         "relatedType": "1",
#                         "fileTitle": null,
#                         "fileOrder": null,
#                     },
#                     {
#                         "id": "af3fb550-429e-11f1-bd9d-fdcce942f5d3",
#                         "fileName": "1628_QD_QD_2026.pdf",
#                         "relatedType": "1",
#                         "fileTitle": "1628_QD_QD_2026.pdf",
#                         "fileOrder": null,
#                     },
#                     {
#                         "id": "b487ca20-429e-11f1-8950-e9b8d09c37df",
#                         "fileName": "af28f900-429e-11f1-ac02-3babf827b65c_content.html",
#                         "relatedType": "5",
#                         "fileTitle": "Quyết định 1628/QĐ-UBND Bãi bỏ toàn bộ Quyết định số 33/2022/QĐ-UBND ngày 08 tháng 8 năm 2022 của Ủy ban nhân dân tỉnh Đồng Nai ban hành quy định về thẩm quyền thẩm định, phê duyệt Kế hoạch ứng phó sự cố tràn dầu cấp cơ sở trên địa bàn tỉnh Đồng Nai",
#                         "fileOrder": null,
#                     },
#                 ],
#                 "sourceDocumentId": null,
#                 "isLw": false,
#             },
#             {
#                 "id": "c94b9430-424b-11f1-b2c9-df1b860af5cf",
#                 "title": "Nghị quyết 66.14/2026/NQ-CP Xử lý khó khăn, vướng mắc trong việc xây dựng, quản lý Cơ sở dữ liệu công chứng",
#                 "docAbs": null,
#                 "docNum": "66.14/2026/NQ-CP",
#                 "docType": {
#                     "id": "044d941c-40de-45b9-ae84-51f5a730bfe0",
#                     "name": "Nghị quyết",
#                     "code": "NQ",
#                 },
#                 "issueDate": "2026-02-10T00:00:00",
#                 "effFrom": "2026-02-10T00:00:00",
#                 "effTo": "2027-02-28T00:00:00",
#                 "publicDate": null,
#                 "updatedDate": "2026-04-28T09:44:07.825967",
#                 "effStatus": {
#                     "id": "1419f6be-4a15-44a7-97ac-ea042770a514",
#                     "code": "CHL",
#                     "name": "Còn hiệu lực",
#                 },
#                 "documentMajors": [
#                     {
#                         "id": "105d3cb0-42ac-11f1-965e-1188fcb8e15b",
#                         "majorType": {
#                             "code": "nganh_65",
#                             "name": "Tư pháp",
#                             "nameEn": null,
#                             "shortName": null,
#                         },
#                         "fieldType": null,
#                     }
#                 ],
#                 "isNew": false,
#                 "documentRelatedList": [
#                     {
#                         "id": "c9588c80-424b-11f1-9f98-abfa78ec04e3",
#                         "fileName": "2026_122_66.14_2026_NQ-CP.pdf",
#                         "relatedType": "1",
#                         "fileTitle": null,
#                         "fileOrder": null,
#                     },
#                     {
#                         "id": "c95b99c0-424b-11f1-8423-1b954a713136",
#                         "fileName": "2026_122_66.14_2026_NQ-CP.docx",
#                         "relatedType": "2",
#                         "fileTitle": "2026_122_66.14_2026_NQ-CP.docx",
#                         "fileOrder": null,
#                     },
#                     {
#                         "id": "c9ad1520-424b-11f1-8240-ada3d998381f",
#                         "fileName": "c94b9430-424b-11f1-b2c9-df1b860af5cf_content.html",
#                         "relatedType": "5",
#                         "fileTitle": "Document c94b9430-424b-11f1-b2c9-df1b860af5cf",
#                         "fileOrder": null,
#                     },
#                 ],
#                 "sourceDocumentId": null,
#                 "isLw": true,
#             },
#         ],
#     },
#     "total": 1,
# }
