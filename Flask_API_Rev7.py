# -*- coding: utf-8 -*-
"""
Created on Sat Oct 28 18:51:37 2017

@author: Sriram
"""

from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
import numpy as np
from flask import jsonify
import pandas as pd
import random
import pickle
from pymongo import MongoClient,GEOSPHERE
import pprint
import random, string
from haversine import haversine
app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('trip_id',location='form',type=int,help='Provide the trip_id reference created during the pickup point suggestion')
parser.add_argument('booking longitude',location='form', type=float,help='Provide the longitude value of the booking location in float and it cannot be empty')
parser.add_argument('booking latitude',location='form', type=float,help='Provide the latitude value of the booking location in float and it cannot be empty')
parser.add_argument('pickup longitude',type=float,location='form',help='Provide the longitude value of the trip starting location in float')
parser.add_argument('pickup latitude',type=float,location='form',help='Provide the latitude value of the trip starting location in float')
parser.add_argument('suggestion hit',type=bool,location='form',help='Whether the pickup point is chosen from the points suggested (True or False)')
parser.add_argument('api_key',location='form',type=str,help='please append the generated api key')

def randomword(length):
   letters = string.ascii_lowercase
   api_random=''.join(random.choice(letters) for i in range(length))
   return api_random

def Selecting_points(nearby_points):
    initial=True
    final_list=[]
    num_cluster_list=[] # list containing the no of points in clusters
    random_index=np.random.randint(0,len(nearby_points))
    random_point=nearby_points[random_index]
    while (len(nearby_points)>0):
        if initial:
            first_point=random_point
            initial=False
        else:
            first_point=len_points[-1,:-1]
        length=[]
        for point in nearby_points:
            if point[0]==first_point[0] and point[1]==first_point[1]:
                length.append(0)
                continue
            second_point=point 
            distance=haversine(first_point,second_point)* 1000
            length.append(distance)
        len_points=np.column_stack((nearby_points,length))
        len_points=len_points[np.argsort(len_points[:,2])] #sorting points according to the distance from the first point
        len_points=len_points[1:]
        cluster=[]
        num_cluster=1
        for point in len_points:

            if point[2]<=40: # checking points nearby
                cluster.append(point[:-1])  # Appending the point nearby to the cluster
                num_cluster+=1
                len_points=np.delete(len_points,0,0) # deleting the points from the list 
        if cluster:
            dist_cluster=[]
            x = [p[0] for p in cluster]
            y = [p[1] for p in cluster]
            centroid = [sum(x) / len(cluster), sum(y) / len(cluster)] #finding the centroid of the cluster
            for point in cluster:
                dist=haversine(centroid,point)* 1000
                dist_cluster.append(dist)
            dist_cluster=np.column_stack((cluster,dist_cluster))
            dist_cluster=dist_cluster[np.argsort(dist_cluster[:,2])] # sorting according to distance
            final_list.append(dist_cluster[0,:-1])  #Choosing the nearest from the centroid
            nearby_points=len_points[:,:-1]
        else:
            final_list.append(first_point)
            nearby_points=len_points[:,:-1]
            #print len(nearby_points)
        num_cluster_list.append(num_cluster)  #logging the number of points in the cluster to weigh the clusters later
    final_list=np.column_stack((final_list,num_cluster_list))
    final_list=final_list[np.argsort(final_list[:,2])] # sorting according to popularity
    print ("suggested points are", final_list[-4:,:-1]) #suggesting the popular pickup points
    #print cluster
    return (final_list[-4:,:-1])

client = MongoClient('mongodb://localhost:27017/')
db = client.location_points
collection = db.Myanmar
collection.drop()
pickup_latitude,pickup_longitude=pickle.load(open('Myanmar_trip_data_Rev2.p','rb')) #Path to this file
#rip_myanmar=pd.read_csv('C:/Users/Administrator/Documents/Database/Myanmar/trip_lat_long_list.csv')
#trip_myanmar.rename(columns={'passengers_log_idÂ\xa0':'passengers_log_id','pickup_lattitude':'pickup_latitude', 'pickup_logitude':'pickup_longitude'},inplace=True)
#trip_myanmar=trip_myanmar.dropna(axis=0,how='any')
for lat,long in zip(pickup_latitude,pickup_longitude):
    lat=float(lat[1:-1])
    long=float(long[1:-1])
    booking_lat=random.uniform(lat,lat+0.001)
    booking_long=random.uniform(long,long+0.001)
    collection.insert_one({"booking_location":{"type": "Point","coordinates": [booking_long,booking_lat]}, "pickup_location":[lat,long]})
    
collection.create_index([("booking_location",GEOSPHERE)])
print("collection count is",collection.count())

#to append all keys to the list
api_key_list=[]

class Authenticate(Resource): #API key generation for each instance
    #developer's password for string 
    dev_pass="ndot"
    def get(self,password):
        if self.dev_pass==password:
            api_key=randomword(24)
            api_key_list.append(api_key)
        else:
            abort(400,message="Password is incorrect. Try again or contact your service provider")
        return jsonify({"message": "Unique API key has been generated","API Key":api_key})

class Suggest_points(Resource):
    trip_id=0
    def post(self):
        Suggest_points.trip_id+=1
        args = parser.parse_args()
        
        try:
            api_key=args['api_key']
        except:
            abort(400, message='Please provide API key along with the request')
        if api_key in api_key_list:
            pass
        else:
            abort(400, message='Please provide the correct API key')
        try:
            point=[args['booking longitude'], args['booking latitude']]
        except:
            abort(400, message='Please give booking location details correctly')
        nearby=[]
        nearby_count=0
        for doc in collection.find({"booking_location": {"$near": { "$geometry" : {"type":"Point","coordinates":point},"$maxDistance":200}}}):
            try:
                nearby.append(doc["pickup_location"])   # appending only if the pickup location field is available
                nearby_count+=1
            except:
                pass
        if nearby:
            if nearby_count>1:
                suggested_points = Selecting_points(nearby)
                suggested_points=suggested_points.tolist()
                collection.insert_one({"trip_id":Suggest_points.trip_id, "booking_location":{"type": "Point","coordinates": point}, "suggested_points":suggested_points})
                return jsonify({"message": "Trip ID created in the database.", "trip_id": Suggest_points.trip_id, "Suggested Points" : suggested_points})
            else:
                suggested_points=nearby
                return jsonify({"message": "Trip ID created in the database.", "trip_id": Suggest_points.trip_id, "Suggested Points" : suggested_points})
        else:
            collection.insert_one({"trip_id":Suggest_points.trip_id, "booking_location":{"type": "Point","coordinates": point}, "suggested_points":"None"})
            return jsonify({"message": "Trip ID created in the database.", "trip_id": Suggest_points.trip_id, "Suggested Points" : "No points nearby"})                    

class Update_trip(Resource):
    def post(self):
        args = parser.parse_args()
        
        try:
            api_key=args['api_key']
        except:
            abort(400, message='Please provide API key along with the request')
        if api_key in api_key_list:
            pass
        else:
            abort(400, message='Please provide the correct API key')
        try:
            trip_id=args['trip_id']
        except:
            abort(400, message='Please send the trip_id reference')
        try:
            pickup_location=[args['pickup longitude'], args['pickup latitude']]
        except:
            abort(400, message='Please give pickup location details correctly')
        try:
            suggestion_hit=args['suggestion hit']
        except:
            abort(400, message='Please specify whether pickup location is chosen from the suggested points')
        
        neighbors=0
        booking_location=collection.find_one({'trip_id':trip_id})['booking_location']['coordinates']
        for doc in collection.find({"booking_location": {"$near": { "$geometry" : {"type":"Point","coordinates":booking_location},"$maxDistance":100}}}):
            neighbors+=1
        
        if neighbors<30: #checking for density nearby
            collection.update_one({"trip_id":trip_id},{"$set":{"pickup_location":pickup_location, "suggestion_hit":suggestion_hit}})
            return jsonify({"message":"Trip details updated"})
        else:
            return jsonify({"message":"Population density already satisfied"})

class Cleanse(Resource): #Cleaning the database of documents of incomplete trips
    def get(self):
        collection.delete_many({"pickup_location": { "$exists": False}})
        return jsonify({"message":"Database cleaned"})

api.add_resource(Authenticate,'/authenticate/<string:password>')
api.add_resource(Suggest_points, '/suggest_points')
api.add_resource(Update_trip, '/update_trip')
api.add_resource(Cleanse,'/clean_database')

if __name__ == '__main__':
    app.run(debug=True) #Running in local host in port 5000. Put host="0.0.0.0" for it to listen publicly
