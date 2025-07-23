# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import os

from flask import Flask, request

import sqlalchemy

from connect_connector import connect_with_connector

BUSINESSES = 'businesses'
REVIEWS = 'reviews'
BUSINESS_NOT_FOUND = {'Error' : 'No business with this business_id exists'}
REVIEW_NOT_FOUND = {'Error': 'No review with this review_id exists'}

app = Flask(__name__)
logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

# Create 'businesses' Table in Database if it Does Not Already Exist
def create_business_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS businesses '                        
                '(business_id INT UNSIGNED AUTO_INCREMENT NOT NULL, '           # Auto_Increment Unsigned Int to match with reviews table 'business_id' attribute
                'owner_id INT NOT NULL, '
                'name VARCHAR(50) NOT NULL, '                                   # Limit to 50 characters
                'street_address VARCHAR(100) NOT NULL, '                        # Limit to 100 characters
                'city VARCHAR(50) NOT NULL, '                                   # Limit to 50 characters
                'state VARCHAR(2) NOT NULL, '
                'zip_code INTEGER NOT NULL, '
                'PRIMARY KEY (business_id) );'
            )
        )
        # Commit transaction to database
        conn.commit()


@app.route('/')
def index():
    return 'Please navigate to /businesses to use this API'

# Create a business
@app.route('/' + BUSINESSES, methods=['POST'])
def create_business():
    business_content = request.get_json()

    # Error handling for missing fields
    business_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
    for field in business_fields:
        if field not in business_content:
            return {"Error" : "The request body is missing at least one of the required attributes"}, 400

    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            # Preparing a statement before hand can help protect against injections.
            stmt = sqlalchemy.text(
                'INSERT INTO businesses(owner_id, name, street_address, city, state, zip_code) '
                ' VALUES (:owner_id, :name, :street_address, :city, :state, :zip_code)'
            )
            # connection.execute() automatically starts a transaction
            conn.execute(stmt, parameters={
                        'owner_id': business_content['owner_id'], 
                        'name': business_content['name'], 
                        'street_address': business_content['street_address'], 
                        'city': business_content['city'], 
                        'state': business_content['state'], 
                        'zip_code': business_content['zip_code']})
            
            # The function last_insert_id() returns the most recent value
            # generated for an `AUTO_INCREMENT` column when the INSERT 
            # statement is executed
            stmt2 = sqlalchemy.text('SELECT last_insert_id()')

            # scalar() returns the first column of the first row or None if there are no rows
            business_id = conn.execute(stmt2).scalar()

            # Commit transaction to database
            conn.commit()

    # Handle connection errors
    except Exception as e:
        logger.exception(e)
        return ({'Error': 'Unable to create lodging'}, 500)
    
    # Create URL for current business
    business_url = request.host_url + BUSINESSES + '/' + str(business_id)

    # Return business to user and send 201 code
    return ({
        'id': business_id,
        'owner_id': business_content['owner_id'], 
        'name': business_content['name'], 
        'street_address': business_content['street_address'], 
        'city': business_content['city'], 
        'state': business_content['state'], 
        'zip_code': business_content['zip_code'],
        'self': business_url}, 201)


# Get a single business
@app.route('/' + BUSINESSES + '/<int:id>', methods=['GET'])
def get_business(id):
    with db.connect() as conn:

        # Check if business exists
        stmt = sqlalchemy.text(
                'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE business_id=:business_id'
            )
        # one_or_none returns at most one result or raise an exception.
        # returns None if the result has no rows.
        row = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
        if row is None:
            return BUSINESS_NOT_FOUND, 404
        
        # Return business and 200 code
        else:
            business = row._asdict()
            del business['business_id']                                                         # Delete business_id and set 'id' field to match
            business['id'] = id                                                                 # expected output
            business['self'] = request.url_root + BUSINESSES + '/' + str(business['id'])
            return business, 200
        

# Get all businesses
@app.route('/' + BUSINESSES, methods=['GET'])
def get_all_businesses():
    # Get offset and limit for pagination
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 3))

    with db.connect() as conn:

        # Create and execute SQL Query to get all businesses in ascending order
        stmt = sqlalchemy.text(
                'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses ORDER BY business_id LIMIT :limit OFFSET :offset'
            )
        rows = conn.execute(stmt, parameters={'limit': limit, 'offset': offset}).mappings()     # Use .mappings() to convert each row to dictionary object

        businesses = []
        # Iterate through the result and add each business to array
        for row in rows:
            business = {
                'id': row['business_id'],
                'owner_id': row['owner_id'],
                'name': row['name'],
                'street_address': row['street_address'],
                'city': row['city'],
                'state': row['state'],
                'zip_code': row['zip_code'],
                'self': request.url_root + BUSINESSES + '/' + str(row['business_id'])
            }
            businesses.append(business)
        
        # Create 'next' link if necessary
        if len(businesses) < limit:
            next = None
        else:
            offset = offset + limit
            next = request.url_root + BUSINESSES + "?limit=" + str(limit) + "&offset=" + str(offset)

        # Return paginated list of businesses to user and send 200 code
        return ({
            'entries': businesses,
            'next' : next
        }, 200)



# Update a business
@app.route('/' + BUSINESSES + '/<int:id>', methods=['PUT'])
def edit_business(id):
     content = request.get_json()

     # Error handling for missing fields
     business_fields = ['owner_id', 'name', 'street_address', 'city', 'state', 'zip_code']
     for field in business_fields:
        if field not in content:
            return {"Error" : "The request body is missing at least one of the required attributes"}, 400
        

     with db.connect() as conn:

        # Check to ensure business exists
        stmt = sqlalchemy.text(
                'SELECT * FROM businesses WHERE business_id=:business_id'
            )
        business = conn.execute(stmt, parameters={'business_id': id}).one_or_none()
        if business is None:
            return BUSINESS_NOT_FOUND, 404
        else:

            # Create SQL query and execute it
            stmt = sqlalchemy.text(
                'UPDATE businesses '
                'SET owner_id = :owner_id, name = :name, street_address = :street_address, city = :city, state = :state, zip_code = :zip_code '
                'WHERE business_id = :business_id'
            )
            conn.execute(stmt, parameters={'owner_id': content['owner_id'], 
                                           'name': content['name'], 
                                            'street_address': content['street_address'], 
                                            'city': content['city'],
                                            'state': content['state'], 
                                            'zip_code': content['zip_code'], 
                                            'business_id': id})
            conn.commit()

            # Construct business URL
            business_url = request.url_root + BUSINESSES + '/' + str(id)

            # Return updated business and 200 code
            return ({
                    'id': id, 
                    'owner_id': content['owner_id'], 
                    'name': content['name'], 
                    'street_address': content['street_address'], 
                    'city': content['city'],
                    'state': content['state'], 
                    'zip_code': content['zip_code'], 
                    'self': business_url}, 200)

# Delete a Business
@app.route('/' + BUSINESSES + '/<int:id>', methods=['DELETE'])
def delete_lodging(id):
     with db.connect() as conn:

        # Create and execute SQL DELETE query
        stmt = sqlalchemy.text(
                'DELETE FROM businesses WHERE business_id=:business_id'
            )
        result = conn.execute(stmt, parameters={'business_id': id})
        conn.commit()
        # result.rowcount value will be the number of rows deleted.
        # For our statement, the value be 0 or 1 because business_id is
        # the PRIMARY KEY
        if result.rowcount == 1:
            return ('', 204)
        else:
            return BUSINESS_NOT_FOUND, 404   


# List all Businesses for an Owner
@app.route('/owners/<int:owner_id>/' + BUSINESSES, methods=['GET'])
def businesses_for_owner(owner_id):
    with db.connect() as conn:

        # Create and execute SQL query to find all businesses for an owner (no pagination)
        stmt = sqlalchemy.text(
            'SELECT business_id, owner_id, name, street_address, city, state, zip_code FROM businesses WHERE owner_id = :owner_id '
        )
        result = conn.execute(stmt, parameters={'owner_id': owner_id}).mappings()

        # Iterate through results and create an array to return
        businesses = []
        for row in result:
            business = {
                'id': row['business_id'], 
                'owner_id': row['owner_id'], 
                'name': row['name'], 
                'street_address': row['street_address'], 
                'city': row['city'], 
                'state': row['state'], 
                'zip_code': row['zip_code'], 
                'self' : request.url_root + BUSINESSES + '/' + str(row['business_id'])
            }
            businesses.append(business)
        
        # Return array of businesses and 200 code
        return businesses, 200


# ————————————————————————————————————————————————————————————————— #
#                               Reviews                             #
# ————————————————————————————————————————————————————————————————— #


# Create 'businesses' Table in Database if it Does Not Already Exist
def create_reviews_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS reviews '
                '(review_id INT UNSIGNED AUTO_INCREMENT NOT NULL, '
                'user_id INT NOT NULL, '
                'business_id INT UNSIGNED NOT NULL , '                                                  # Unsigned Int to match business_id in businesses table
                'stars SMALLINT NOT NULL, '
                'review_text VARCHAR(1000), '                                                           # Limit to 1000 characters
                'PRIMARY KEY (review_id), '
                'FOREIGN KEY (business_id) REFERENCES businesses(business_id) ON DELETE CASCADE ); '    # ON DELETE CASCADE to remove all reviews
            )                                                                                           # for a business when it is deleted
        )
        conn.commit()


# Create a Review
@app.route('/' + REVIEWS, methods=['POST'])
def create_review():
    content = request.get_json()

    # Error handling for missing fields
    review_fields = ['user_id', 'business_id', 'stars']
    for field in review_fields:
        if field not in content:
            return {"Error" : "The request body is missing at least one of the required attributes"}, 400

    try:
        with db.connect() as conn:

            # Ensure business exists
            business_exists_query = sqlalchemy.text(
                'SELECT 1 FROM businesses WHERE business_id = :business_id '
            )
            existing_business = conn.execute(business_exists_query, {'business_id': content['business_id']}).scalar()
            if existing_business is None:
                return BUSINESS_NOT_FOUND, 404
            

            # Ensure no existing reviews by this user for this business
            review_exists_query = sqlalchemy.text(
                'SELECT 1 FROM reviews WHERE business_id = :business_id AND user_id = :user_id '
            )
            existing_review = conn.execute(review_exists_query, {'business_id': content['business_id'], 'user_id': content['user_id']}).scalar()
            if existing_review is not None:
                return {"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}, 409
            
            # Create and execute SQL query to make a new review
            if 'review_text' in content:
                review_text = content['review_text']
            else:
                review_text = ''
            new_review_stmt = sqlalchemy.text(
                'INSERT INTO reviews (user_id, business_id, stars, review_text)'
                ' VALUES (:user_id, :business_id, :stars, :review_text)'
            )
            conn.execute(new_review_stmt, parameters={'user_id': content['user_id'], 'business_id': content['business_id'], 'stars': content['stars'], 'review_text': review_text})
            
            # The function last_insert_id() returns the most recent value
            # generated for an `AUTO_INCREMENT` column when the INSERT 
            # statement is executed
            stmt2 = sqlalchemy.text('SELECT last_insert_id()')
            
            # scalar() returns the first column of the first row or None if there are no rows
            review_id = conn.execute(stmt2).scalar()
            # Commit the transaction to database
            conn.commit()

    # Handle connection errors
    except Exception as e:
        logger.exception(e)
        return ({'Error': 'Unable to create lodging'}, 500)
    
    # Create business and review URLs to return
    business_url = request.host_url + BUSINESSES + '/' + str(content['business_id'])
    review_url = request.host_url + REVIEWS + '/' + str(review_id)

    # Return review and 201 code
    return ({
        'id': review_id,
        'user_id': content['user_id'], 
        'business': business_url, 
        'stars': content['stars'], 
        'review_text': review_text, 
        'self': review_url}, 201)
            

# Get a Review
@app.route('/' + REVIEWS + '/<int:id>', methods=['GET'])
def get_review(id):
    with db.connect() as conn:

        # Ensure review exists
        stmt = sqlalchemy.text(
                'SELECT review_id, user_id, business_id, stars, review_text FROM reviews where review_id = :review_id'
            )
        # one_or_none returns at most one result or raise an exception.
        # returns None if the result has no rows.
        row = conn.execute(stmt, parameters={'review_id': id}).one_or_none()
        if row is None:
            return REVIEW_NOT_FOUND, 404
        else:
            # Create business and review URLs and return review to user
            review = row._asdict()
            review['business'] = request.url_root + BUSINESSES + '/' + str(review['business_id'])
            del review['business_id']                                                                   # Delete business_id and review_id and set 'id' field to match
            review['id'] = review['review_id']                                                          # expected output
            del review['review_id']
            review['self'] = request.url_root + REVIEWS + '/' + str(id)
            return review, 200
        

# Edit a Review
@app.route('/' + REVIEWS + '/<int:id>', methods=["PUT"])
def edit_review(id):
    content = request.get_json()

    # Ensure 'stars' attribute is in request
    if 'stars' not in content:
        return {'Error': 'The request body is missing at least one of the required attributes'}, 400
    
    with db.connect() as conn:

        # Ensure review exists
        stmt = sqlalchemy.text(
            'SELECT * FROM reviews WHERE review_id = :review_id '
        )
        row = conn.execute(stmt, parameters={'review_id': id}).mappings().one_or_none()
        if row is None:
            return REVIEW_NOT_FOUND, 404
        else:
            # Update review_text if necessary
            if 'review_text' in content:
                review_text = content['review_text']
            else:
                review_text = row['review_text']

            # Create and execute SQL UPDATE query for review    
            update_stmt = sqlalchemy.text(
                'UPDATE reviews '
                'SET stars = :stars, review_text = :review_text '
                'WHERE review_id = :review_id '
            )
            conn.execute(update_stmt, parameters={'review_id': id, 'stars': content['stars'], 'review_text': review_text})
            conn.commit()

            # Create business and review URLs
            business_url = request.url_root + BUSINESSES + '/' + str(row['business_id'])
            review_url = request.url_root + REVIEWS + '/' + str(id)

            # Return updated review and 200 code
            return ({
                'id': id, 
                'user_id': row['user_id'], 
                'business': business_url,
                'stars': content['stars'], 
                'review_text': review_text, 
                'self': review_url
            }, 200)
        

# Delete a Review
@app.route('/' + REVIEWS + '/<int:id>', methods=['DELETE'])
def delete_review(id):
    with db.connect() as conn:
        delete_stmt = sqlalchemy.text(
            'DELETE FROM reviews WHERE review_id = :review_id'
        )
        result = conn.execute(delete_stmt, parameters={'review_id': id})
        conn.commit()

        # Delete review if it exists, send 404 otherwise
        if result.rowcount == 1:
            return ('', 204)
        else:
            return REVIEW_NOT_FOUND, 404 


# List all Reviews for a User
@app.route('/users/<int:user_id>/' + REVIEWS, methods=['GET'])
def reviews_for_user(user_id):
    with db.connect() as conn:

        # Create and Execute SQL query to find all reviews by a user
        stmt = sqlalchemy.text(
            'SELECT review_id, user_id, business_id, stars, review_text FROM reviews WHERE user_id = :user_id '
        )
        result = conn.execute(stmt, parameters={'user_id': user_id}).mappings()

        # Create array of reviews (with business and review URLs) and return with 200 code
        reviews = []
        for row in result:
            review = {
                'id': row['review_id'], 
                'user_id': row['user_id'], 
                'business': request.url_root + BUSINESSES + '/' + str(row['business_id']), 
                'stars': row['stars'], 
                'review_text': row['review_text'], 
                'self' : request.url_root + REVIEWS + '/' + str(row['review_id'])
            }
            reviews.append(review)
        
        return reviews, 200



if __name__ == '__main__':
    init_db()
    create_business_table(db)
    create_reviews_table(db)
    app.run(host='0.0.0.0', port=8080, debug=True)
