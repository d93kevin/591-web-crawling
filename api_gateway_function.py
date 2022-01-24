import pandas
import boto3
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("591-housing-historical")
s3 = boto3.client("s3")

def lambda_handler(event, context):
    # 檢查有無日期參數
    try:
        result = []
        count = 0
        # 檢查是否僅有日期參數
        if event["queryStringParameters"]["日期"] and len(event["queryStringParameters"])==1:
            response = table.query(
                                KeyConditionExpression=Key("日期").eq(event["queryStringParameters"]["日期"])
                                )
            result.extend(response["Items"])
            count += response["Count"]
            # dynamodb的每一次query有1MB的限制，透過"LastEvaluatedKey"和"ExclusiveStartKey"能把剩餘的取回來
            while "LastEvaluatedKey" in response:
                # 從"ExclusiveStartKey"開始取資料
                response = table.query(
                                    KeyConditionExpression=Key("日期").eq(event["queryStringParameters"]["日期"]),
                                    ExclusiveStartKey=response["LastEvaluatedKey"]
                                    )
                result.extend(response["Items"])
                count += response["Count"]
        else:
            filterexpression = ""
            for key in event["queryStringParameters"]:
                if key != "日期":
                    filterexpression += f'Attr("{key}").eq(event["queryStringParameters"]["{key}"])&'
            response = table.query(
                                KeyConditionExpression=Key("日期").eq(event["queryStringParameters"]["日期"]),
                                FilterExpression=eval(filterexpression.strip("&"))
                                )
            result.extend(response["Items"])
            count += response["Count"]
            while "LastEvaluatedKey" in response:
                response = table.query(
                                    KeyConditionExpression=Key("日期").eq(event["queryStringParameters"]["日期"]),
                                    FilterExpression=eval(filterexpression.strip("&")),
                                    ExclusiveStartKey=response["LastEvaluatedKey"]
                                    )
                result.extend(response["Items"])
                count += response["Count"]
        data = pandas.DataFrame(result)
        # 檔名以參數命名
        object_name = ""
        for key in event["queryStringParameters"]:
            object_name += f'{event["queryStringParameters"][key]}-'
        # 資料存成csv檔
        data_csv = data.to_csv(index=False)
        # 將請求資料存進s3 bucket
        bucket_name = "591-requested-data"
        s3.put_object(Body=data_csv, Bucket=bucket_name, Key=f'{object_name.strip("-")}.csv')
        return {"Message": f"{count} data have been retrieved and store in s3 bucket"}
    except:
        return {"Message": "Please choose a date as parameter"}