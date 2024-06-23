import os

from flask import Flask, render_template, request, Response, jsonify
from google.cloud import datastore

os.environ["GCLOUD_PROJECT"] = "cs473assignmentonekevinbraman"
datastore_client = datastore.Client()

def store_business_info(owner_id, name, street_address, city, state, zip_code):
    entity = datastore.Entity(key=datastore_client.key("business_info"))
    entity.update({
        "owner_id": owner_id,
        "name": name,
        "street_address": street_address,
        "city": city,
        "state": state,
        "zip_code": zip_code
    })
    datastore_client.put(entity)
    return entity

def store_reviews(user_id, business_id, stars, review_text):
    entity = datastore.Entity(key=datastore_client.key("review_info"))
    entity.update({
        "user_id": user_id,
        "business_id": business_id,
        "stars": stars,
        "review_text": review_text 
    })
    datastore_client.put(entity)
    return entity


app = Flask(__name__)

# CREATE A BUSINESS
@app.route('/businesses', methods=['POST'])
def create_business():
    data = request.get_json()
    required_fields = ["owner_id", "name", "street_address", "city", "state", "zip_code"]
    if not all(field in data for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400
    entity = store_business_info(owner_id=data['owner_id'],
        name=data['name'],
        street_address=data['street_address'],
        city=data['city'],
        state=data['state'],
        zip_code=data['zip_code']
    )
    response= {
            "id": entity.key.id,
            "owner_id": entity["owner_id"],
            "name": entity["name"],
            "street_address": entity["street_address"],
            "city": entity["city"],
            "state": entity["state"],
            "zip_code": entity["zip_code"]
        }
    return jsonify(response), 201


# GET A BUSINESS
@app.route('/businesses/<int:business_id>', methods=['GET'])
def get_business(business_id):
    key = datastore_client.key('business_info', business_id)
    business = datastore_client.get(key)
    
    if business:
        business_data = {
            "owner_id": business['owner_id'],
            "name": business['name'],
            "street_address": business['street_address'],
            "city": business['city'],
            "state": business['state'],
            "zip_code": business['zip_code']
        }
        return jsonify(business_data), 200
    else:
        return jsonify({"Error": "No business with this business_id exists"}), 404

# LIST ALL BUSINESSES
@app.route('/businesses', methods=['GET'])
def list_businesses():
    query = datastore_client.query(kind='business_info')
    results = list(query.fetch())
    businesses = []
    for business in results:
        business_data = {
            "business_id": business.key.id,
            "owner_id": business["owner_id"],
            "name": business["name"],
            "street_address": business["street_address"],
            "city": business["city"],
            "state": business["state"],
            "zip_code": business["zip_code"]
        }
        businesses.append(business_data)

    return jsonify(businesses), 200

#EDIT A BUSINESS 
@app.route('/businesses/<int:business_id>', methods=['PUT'])
def edit_business(business_id):
    data = request.get_json()
    required_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    if not all(field in data for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400
    
    key = datastore_client.key('business_info', business_id)
    business = datastore_client.get(key) 
    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    business.update({
        "owner_id": data['owner_id'],
        "name": data['name'],
        "street_address": data['street_address'],
        "city": data['city'],
        "state": data['state'],
        "zip_code": data['zip_code']
    })
    datastore_client.put(business)
    response = {
        "id": business_id,
        "owner_id": business['owner_id'],
        "name": business['name'],
        "street_address": business['street_address'],
        "city": business['city'],
        "state": business['state'],
        "zip_code": business['zip_code']
    }

    return jsonify(response), 200

# DELETE A BUSINESS
@app.route('/businesses/<int:business_id>', methods=['DELETE'])
def delete_business(business_id):
    business_key = datastore_client.key('business_info', business_id)
    business = datastore_client.get(business_key)
    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    query = datastore_client.query(kind='review_info')
    query.add_filter('business_id', '=', business_id)
    reviews = list(query.fetch())
    for review in reviews:
        datastore_client.delete(review.key)

    datastore_client.delete(business_key)

    return Response(status=204)

# GET ALL BUSINESSES FOR AN OWNER
@app.route('/owners/<int:owner_id>/businesses', methods=['GET'])
def list_businesses_for_owner(owner_id):
    query = datastore_client.query(kind='business_info')
    query.add_filter('owner_id', '=', owner_id)
    results = list(query.fetch())
    businesses = []
    for business in results:
        business_data = {
            "business_id": business.key.id,
            "owner_id": business["owner_id"],
            "name": business["name"],
            "street_address": business["street_address"],
            "city": business["city"],
            "state": business["state"],
            "zip_code": business["zip_code"]
        }
        businesses.append(business_data)

    return jsonify(businesses), 200

# CREATE A REVIEW
@app.route('/reviews', methods=['POST'])
def create_review():
    data = request.get_json()  
    required_fields = ['user_id', 'business_id', 'stars']
    if not all(field in data for field in required_fields):
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    business_key = datastore_client.key('business_info', int(data['business_id']))
    business = datastore_client.get(business_key)
    if not business:
        return jsonify({"Error": "No business with this business_id exists"}), 404

    query = datastore_client.query(kind='review_info')
    query.add_filter('user_id', '=', int(data['user_id']))
    query.add_filter('business_id', '=', int(data['business_id']))
    existing_reviews = list(query.fetch())
    if existing_reviews:
        return jsonify({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), 409

    review_text = data.get('review_text', '')
    entity = store_reviews(data['user_id'], data['business_id'], data['stars'], review_text)
    response = {
        "id": entity.key.id,
        "user_id": entity["user_id"],
        "business_id": entity["business_id"],
        "stars": entity["stars"],
        "review_text": entity.get("review_text", "")
    }

    return jsonify(response), 201

# GET A REVIEW
@app.route('/reviews/<int:review_id>', methods=['GET'])
def get_review(review_id):
    review_key = datastore_client.key('review_info', review_id)
    review = datastore_client.get(review_key)
    if not review:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    review_data = {
        "review_id": review_id,
        "user_id": review["user_id"],
        "business_id": review["business_id"],
        "stars": review["stars"],
        "review_text": review["review_text"]
    }

    return jsonify(review_data), 200

#EDIT A REVIEW
@app.route('/reviews/<int:review_id>', methods=['PUT'])
def edit_review(review_id):
    data = request.get_json()
    if 'stars' not in data:
        return jsonify({"Error": "The request body is missing at least one of the required attributes"}), 400

    key = datastore_client.key('review_info', review_id)
    review = datastore_client.get(key)
    if not review:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    if 'stars' in data:
        review['stars'] = data['stars']
    if 'review_text' in data:
        review['review_text'] = data['review_text']
    
    datastore_client.put(review)
    response = {
        "id": review_id,
        "user_id": review["user_id"],
        "business_id": review["business_id"],
        "stars": review["stars"],
        "review_text": review["review_text"]
    }

    return jsonify(response), 200

# DELETE A REVIEW
@app.route('/reviews/<int:review_id>', methods=['DELETE'])
def delete_review(review_id):
    review_key = datastore_client.key('review_info', review_id)
    review = datastore_client.get(review_key)
    if not review:
        return jsonify({"Error": "No review with this review_id exists"}), 404

    datastore_client.delete(review_key)
    return Response(status=204)

# GET ALL REVIEWS FOR USER
@app.route('/users/<int:user_id>/reviews', methods=['GET'])
def list_reviews_for_user(user_id):
    query = datastore_client.query(kind='review_info')
    query.add_filter('user_id', '=', user_id)
    results = list(query.fetch())
    reviews = []
    for review in results:
        review_data = {
            "id": review.key.id,
            "user_id": review["user_id"],
            "business_id": review["business_id"],
            "stars": review["stars"],
            "review_text": review.get("review_text", "")
        }
        reviews.append(review_data)

    return jsonify(reviews), 200

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)
