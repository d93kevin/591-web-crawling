import boto3
import pandas
import urllib

# s3資源
s3 = boto3.client('s3')
# dynamodb資源
ddb = boto3.resource('dynamodb')
table = ddb.Table("591-housing-historical")

def lambda_handler(event, context):
    # 從s3產生出來的event，取得檔名、bucket等資訊
    object_name = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    # 下載檔案
    s3_object = s3.get_object(Bucket=bucket_name, Key=object_name)
    # 轉換為DataFrame型式
    house_data = pandas.read_csv(s3_object["Body"])
    # 逐筆存到DynamoDB資料表
    for index, data  in house_data.iterrows():
        table.put_item(
            Item={"日期": data["日期"],
                  "物件編號":data["物件編號"],
                  "類型": data["類型"],
                  "型態": data["型態"],
                  # float to string
                  "坪數": str(data["坪數"]),
                  "價格": data["價格"],
                  "行政區": data["行政區"],
                  "性別規定": data["性別規定"],
                  "聯絡人": data["聯絡人"],
                  "聯絡人身分": data["聯絡人身分"],
                  "連絡電話": data["連絡電話"],
                  "冰箱": data["冰箱"],
                  "洗衣機": data["洗衣機"],
                  "電視": data["電視"],
                  "冷氣": data["冷氣"],
                  "熱水器": data["熱水器"],
                  "床": data["床"],
                  "衣櫃": data["衣櫃"],
                  "第四台": data["第四台"],
                  "網路": data["網路"],
                  "瓦斯": data["瓦斯"],
                  "沙發": data["沙發"],
                  "桌椅": data["桌椅"],
                  "陽台": data["陽台"],
                  "電梯": data["電梯"],
                  "車位": data["車位"]
                 }
        )
    return None