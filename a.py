# import json

# import requests

# url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"

# payload = json.dumps({"pageSize": 20, "pageNumber": 1})
# headers = {
#     # "accept": "application/json",
#     # "accept-language": "en,en-US;q=0.9,vi;q=0.8",
#     # "content-type": "application/json",
#     # "origin": "https://vbpl.vn",
#     # "priority": "u=1, i",
#     # "referer": "https://vbpl.vn/",
#     # "sec-ch-ua": '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
#     # "sec-ch-ua-mobile": "?0",
#     # "sec-ch-ua-platform": '"Windows"',
#     # "sec-fetch-dest": "empty",
#     # "sec-fetch-mode": "cors",
#     # "sec-fetch-site": "cross-site",
#     # "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
#     # "Cookie": "18e31cb783bbe0fed5fcf6b5a18fc158=7059dfd6a4e7a482816033dad3a8052c; cookiesession1=678A3E188E540863D39E0DA1F3FB4745",
# }

# response = requests.request("POST", url, headers=headers, data=payload)

# print(response.text)

import requests

url = "https://vbpl-bientap-gateway.moj.gov.vn/api/qtdc/public/doc/all"

# Truyền thẳng dictionary, không cần json.dumps
payload = {"pageSize": 20, "pageNumber": 1}

# Sử dụng tham số json= thay vì data=
response = requests.request("POST", url, json=payload)

print(response.text)
