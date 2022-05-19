# This file is for utility purposes
import decimal


def convert_to_formats(data):
    try:
        # This should come from db to make it dynamic
        formats = {'year':int,'duration':int,'votes':int,'metascore':int,'reviews_from_users':int,'reviews_from_critics':int}

        # Now convert the data
        for field,cls in formats.items():
            if data[field]:
                data[field] = cls(data[field])
    except Exception as e:
        print(e)

def convert_decimals_for_json(records):
    # This is to convert decimal values to int or float
    for data in records:
        for key in data:
            if isinstance(data[key],decimal.Decimal):
                if data[key] % 1 == 0:
                    data[key] = int(data[key])
                else:
                    data[key] = float(data[key])

def process_currency(records,keyToProcess):
    for data in records:
        if data[keyToProcess]:
            data[keyToProcess] = int(''.join(i for i in data[keyToProcess] if i.isdigit()))
        else:
            data[keyToProcess] = 0          # To handle blank entries

def get_return_response(response):
    if response[0] == 200:
        return response[1]
    elif response[0] == 404:
        return response[1]
    else:
        return {'errorMsg':'Internal Server Error'}