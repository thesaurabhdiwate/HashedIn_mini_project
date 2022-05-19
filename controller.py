from boto3 import resource
from boto3.dynamodb.conditions import Attr,Key
import config
import constants
import utils


res = resource(
    'dynamodb',
    aws_access_key_id = config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key = config.AWS_SECRET_ACCESS_KEY,
    region_name = config.REGION_NAME,
    endpoint_url = config.ENDPOINT_URL
)

movieTable = res.Table('Movie')



def create_table_movie():
    try:
        table = res.create_table(
            TableName = constants.MOVIE_TABLE,
            KeySchema = [
                {
                    'AttributeName': constants.MOVIE_ID,
                    'KeyType'      : 'HASH'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName': constants.MOVIE_ID, # Name of the attribute
                    'AttributeType': 'S'   # N = Number (B= Binary, S = String)
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits'  : 10,
                'WriteCapacityUnits': 10
            }

        )
    except Exception as e:
        print('Exception(create_table_movie):')
        print(e)
        return None
    
    return table

def add_records_from_file():
    try:
        with open('Movie2.csv','r',encoding='utf-8') as file:
            movielist = [list(map(str.strip,line.split('\t'))) for line in file.readlines()]
    
        keys = movielist[0]                  # got the field value

        print('movielist created')

        for movieData in movielist[1:]:
            row = dict(list(zip(keys,movieData)))
            utils.convert_to_formats(row)
            response = insert_movie(row)

    except Exception as e:
        print('could not add movies')
        return "Error in adding movies to db"


def delete_table_movie():
    try:
        response = res.delete_table(TableName=constants.MOVIE_TABLE)
        print(response)
    except Exception as e:
        print(e)


def insert_movie(row):
    try:
        response = movieTable.put_item(Item=row)
        return response
    except Exception as e:
        print(e)
        return "Could not insert into table movie."


def get_titles_by_director(name,yearRange):
    try:
        years = list(map(int,yearRange.split('-'))) 
        years.sort()                                         # This will give years from,to format
        response = movieTable.scan(
            ProjectionExpression = 'title',
            FilterExpression = Attr('director').eq(name) & Attr('year').between(years[0],years[1])
        )
        if 'Items' in response and response['Items']:
            retData = response['Items']
            return 200,retData
        else:
            return 404,"No Data Found"
    except Exception as e:
        print(e)
        return 500,"Could not get item from table movie."

def get_engTitles_by_review(review):
    try:
        #Get data from Db
        response = movieTable.scan(
            FilterExpression = Attr('language').eq('English') & Attr('reviews_from_users').gte(review)
        )
        #process it
        if 'Items' in response and response['Items']:
            retData = sorted(response['Items'], key=lambda x:x['reviews_from_users'], reverse=True)
            utils.convert_decimals_for_json(retData)
            return 200,retData
        else:
            return 404,"No Data Found"

    except Exception as e:
        print(e)
        return 500,"Could not get titles by review."

def get_highest_budget_movie(country,year):
    try:
        #Get data from Db
        response = movieTable.scan(
            ProjectionExpression = 'title,country,#y,budget',
            ExpressionAttributeNames ={
                '#y':'year'
            },
            FilterExpression = Attr('country').eq(country) & Attr('year').eq(year)
        )

        #process it
        if 'Items' in response and response['Items']:
            #process the currency
            utils.process_currency(response['Items'],'budget')
            retData = sorted(response['Items'], key=lambda x:x['budget'], reverse=True)
            utils.convert_decimals_for_json(retData)
            return 200,retData[0]       # Highest only
        else:
            return 404,"No Data Found"    
    except Exception as e:
        print(e)
        return 500,"Could not get highest movie by budget."