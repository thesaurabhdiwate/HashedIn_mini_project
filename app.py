from flask import Flask, request, jsonify
import controller as dynamoDb
import utils
from functools import wraps
from datetime import datetime


app = Flask(__name__)

def token_required(func):
    @wraps(func)
    def check_authToken(*args, **kwargs):
        token = None
        #check if token is present in headers
        if 'x-access-token' in request.headers:
            token = request.headers['x-access-token']

        if not token:
            return jsonify({'message' : 'Token is missing !!'}), 401
        
        return  func(*args, **kwargs)
    
    return check_authToken

def calc_time(func):
    @wraps(func)
    def check_exec_time(*args, **kwargs):
        # get start time
        start = datetime.now()
        response = func(*args, **kwargs)
        # get end time
        end = datetime.now()
        #set the difference in header
        response.headers['x-time-to-execute'] = end - start
        return response
    
    return check_exec_time


@app.route('/getTitlesByDirector/<name>/<yearRange>',methods=['GET'])
@token_required
@calc_time
def getTitlesByDirector(name,yearRange):
    response = dynamoDb.get_titles_by_director(name,yearRange)
    #check response
    return jsonify(utils.get_return_response(response))


@app.route('/getEngTitlesByRating/<int:review>', methods=['GET'])
@token_required
@calc_time
def getTitlesByRating(review):
    #Get data from db    
    response = dynamoDb.get_engTitles_by_review(review)
    #check response
    return jsonify(utils.get_return_response(response))


@app.route('/getHighestBudgetMovie/<country>/<int:year>', methods=['GET'])
@token_required
@calc_time
def getHighestBudgetMovie(country,year):
    #Get data from db
    response = dynamoDb.get_highest_budget_movie(country,year)
    #check response
    return jsonify(utils.get_return_response(response))
    

@app.route('/')
@token_required
@calc_time
def def_route():

    #delete existing table first
    # java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb
    #dynamoDb.delete_table_movie()

    #Now create new one
    table = dynamoDb.create_table_movie()
    if not table:
        return jsonify('ERROR! Could not create movie table!')

    # Now read the csv and populate the table
    dynamoDb.add_records_from_file()

    return jsonify("Table is created and Movies added")
 


if __name__ == '__main__':
    app.run(port=5500,debug=True)