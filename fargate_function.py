import boto3
import pandas
import datetime
import requests
from bs4 import BeautifulSoup
import re

# s3資源
s3 = boto3.client("s3")

def house_crawler(event, context):
    # 今天日期，美國時區
    today = datetime.date.today()
    # 用列表收集各個物件資訊
    post_id = []
    kind = []
    types = []
    area = []
    price = []
    section = []
    rule = []
    contact = []
    role = []
    mobile = []
    fridge = []
    washer = []
    tv = []
    cold = []
    heater = []
    bed = []
    closet = []
    fourth = []
    net = []
    gas = []
    sofa = []
    table_chairs = []
    balcony = []
    lift = []
    park = []
    # 物件總數
    max_records = get_post_list()["records"]
    # 迴圈到最後一頁停止
    for row in range(0, int(max_records.replace(",","")), 30):
        post_list = get_post_list(row)["data"]["data"]
        for post in post_list:
            # 車位不是租屋資訊，而且爬取資訊的欄位並沒有出現在車位物件中，以免報錯
            if post["kind_name"] != "車位":
                post_id.append(post["post_id"])
                kind.append(post["kind_name"])
                section.append(post["section_name"])
                area.append(post["area"])
                # 有"1,000~2,000"和"1,000"這兩種可能格式，得先替換掉"~"和","才能轉為數字
                price.append(int(post["price"].split("~")[-1].replace(",","")))
                contact.append(post["contact"])
                role.append(post["role_name"])
                # 進一步取得詳細資訊
                temp = get_post(post["post_id"])
                types.append(temp["info"][3]["value"])
                # 有些聯絡電話放mobile，有些放phone
                if temp["linkInfo"]["mobile"]:
                    mobile.append(temp["linkInfo"]["mobile"])
                else:
                    mobile.append(temp["linkInfo"]["phone"])
                # 有無提供以下15項設備
                fridge.append(temp["service"]["facility"][0]["active"])
                washer.append(temp["service"]["facility"][1]["active"])
                tv.append(temp["service"]["facility"][2]["active"])
                cold.append(temp["service"]["facility"][3]["active"])
                heater.append(temp["service"]["facility"][4]["active"])
                bed.append(temp["service"]["facility"][5]["active"])
                closet.append(temp["service"]["facility"][6]["active"])
                fourth.append(temp["service"]["facility"][7]["active"])
                net.append(temp["service"]["facility"][8]["active"])
                gas.append(temp["service"]["facility"][9]["active"])
                sofa.append(temp["service"]["facility"][10]["active"])
                table_chairs.append(temp["service"]["facility"][11]["active"])
                balcony.append(temp["service"]["facility"][12]["active"])
                lift.append(temp["service"]["facility"][13]["active"])
                park.append(temp["service"]["facility"][14]["active"])
                # 可能不會有房屋守則，沒有就是男女皆可
                try:
                    rule.append(re.search("此房屋(.*)租住", temp["service"]["rule"]).group(1))
                except:
                    rule.append("男女皆可")
    
    post_data = {"日期": today,
                 "物件編號": post_id,
                 "類型": kind,
                 "型態": types,
                 "坪數": area,
                 "價格": price,
                 "行政區": section,
                 "性別規定": rule,
                 "聯絡人": contact,
                 "聯絡人身分": role,
                 "連絡電話": mobile,
                 "冰箱": fridge,
                 "洗衣機": washer,
                 "電視": tv,
                 "冷氣": cold,
                 "熱水器": heater,
                 "床": bed,
                 "衣櫃": closet,
                 "第四台": fourth,
                 "網路": net,
                 "瓦斯": gas,
                 "沙發": sofa,
                 "桌椅": table_chairs,
                 "陽台": balcony,
                 "電梯": lift,
                 "車位": park
                }
    data = pandas.DataFrame(post_data)
    # 檔名以今天日期命名
    object_name = f"{today.year}年{today.month}月{today.day}日.csv"
    # 所有物件資訊存成csv檔
    data_csv = data.to_csv(index=False)
    # 將今天的租屋物件存進s3
    bucket_name = "591-housing-objects"
    s3.put_object(Body=data_csv, Bucket=bucket_name, Key=object_name)
    return None

def get_post_list(row=0):
    '''
    取得每頁30項的租屋列表
    '''
    headers = {"X-CSRF-TOKEN": csrf_token,
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36",
               "Cookie": f"urlJumpIp=8; 591_new_session={uje_new_session}"
              }
    return session.get(f"https://rent.591.com.tw/home/search/rsList?is_format_data=1&is_new_list=1&type=1&region=8&firstRow={row}", headers=headers).json()

def get_post(post_id):
    '''
    根據物件標號取得詳細資訊
    '''
    headers = {"deviceid": phpsessid,
               "device": "pc",
               "User-Agent": "Mozila/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36"
              } 
    return session.get(f'https://bff.591.com.tw/v1/house/rent/detail?id={post_id}', headers=headers).json()["data"]

if __name__ == '__main__':
    '''
    entry point
    '''
    # 建立Session
    session = requests.Session()
    # 請求原始網頁的封包
    response = session.get("https://rent.591.com.tw/?region=8", headers={"user-agent":"custom"})
    soup = BeautifulSoup(response.text, "html.parser")
    # 以下3個重要資訊會在爬取資料時用上
    csrf_token = soup.find("meta", {"name":"csrf-token"})["content"]
    uje_new_session = response.cookies["591_new_session"]
    phpsessid = response.cookies["PHPSESSID"]
    # 主程式
    house_crawler(None, None)