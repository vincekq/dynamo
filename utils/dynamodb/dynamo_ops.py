# -*- coding: utf-8 -*-
"""A wrapper around DynamoDB using Boto3

This package contains frequently used operations with DynamoDB
This DynamoDB utility package allow us to to extend the behavior of the boto3's methods, 
without permanently modifying it
"""

import boto3


class DynamoOps():
    def __init__(self, table, region_name='eu-west-1'):
        self.client = boto3.resource('dynamodb', region_name)
        self.table = self.client.Table(table)
        

    def putItem(self, item):
        '''
        Creates a new item, or replaces an old item with a new item
           :param str item: The item to put in the dynamodb table
           :return: response: 
       '''
        try:
            response = self.table.put_item(Item=item)
            return response
        except Exception as e:
            print(e)
            
    
    def getItem(self, item):
        '''
        The GetItem operation returns a set of attributes for the item with the given primary key.
           :param str item: The item to query in the dynamodb table. Contains primary key
           :return: response: 
       '''
        try:
            response = self.table.get_item(Key=item)
            return response['Item']
        except Exception as e:
            print(e)
           
    def getAllItems(self):
        try:
            response = self.table.scan()
            data = response['Items']
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])
        except Exception as e:
            print(e)
        else:
            return data
           
    def get_update_params(self, body):
        update_expression = ["set "]
        update_values = dict()
        for key, val in body.items():
            update_expression.append(f" {key} = :{key},")
            update_values[f":{key}"] = val
    
        return "".join(update_expression)[:-1], update_values
        
    def getPrimaryKey(self, keydict):
        key = list(keydict.keys())
        primarykey = key[0]
        return primarykey
    
    def updateItem(self, keydict, body):
        primarykey = self.getPrimaryKey(keydict)
        update_expression, update_values = self.get_update_params(body)
        try:
            response = self.table.update_item(
                Key=keydict,
                UpdateExpression=update_expression,
                ExpressionAttributeValues=dict(update_values),
                ConditionExpression="attribute_exists({})".format(primarykey)
            )
        except Exception as error:
            print(error)
            return
        return response
           
    def deleteItem(self, keydict):
        primarykey = self.getPrimaryKey(keydict)
        try:
            response =self.table.delete_item(Key=keydict,ConditionExpression="attribute_exists({})".format(primarykey))
            print(response)
        except Exception as e:
            print(e)
        else:
            return response