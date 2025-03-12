
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import mysql.connector

app = Flask(__name__)
CORS(app)

# Connect to MySQL
db = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="pass",
    database="sakila"
)
cursor = db.cursor(dictionary=True)

@app.route('/films', methods=['GET'])
def get_films():
    cursor.execute("""SELECT * FROM sakila.film_list;""")
    response = jsonify(cursor.fetchall())
    return response

@app.route('/top5films', methods=['GET'])
def get_t5f():
    cursor.execute("""select f.film_id as ID, f.title as TITLE, c.name as GENRE, count(r.rental_id) as RENTALS, f.description as "DESC", f.length as LENGTH, f.rating as RATING, fl.ACTORS as ACTORS from rental as r
        join inventory as i
        on r.inventory_id = i.inventory_id
        join film as f
        on i.film_id = f.film_id
        join film_category as fc
        on f.film_id = fc.film_id
        join category as c
        on fc.category_id = c.category_id
        join film_list as fl
        on f.film_id = fl.ID
        group by i.film_id, c.name, fl.actors
        order by RENTALS desc
        limit 5;""")
    response = jsonify(cursor.fetchall())
    return response

@app.route('/top5actors', methods=['GET'])
def get_t5a():
    cursor.execute("""select a.actor_id as ID, a.first_name as FIRST, a.last_name as LAST, count(fa.film_id) as FILMCOUNT from actor as a
        join film_actor as fa
        on a.actor_id = fa.actor_id
        group by fa.actor_id
        order by filmCount desc
        limit 5;""")
    response = jsonify(cursor.fetchall())
    return response

@app.route('/actor5films', methods=['GET'])
def get_a5f():
    actorID = request.args.get('id')
    cursor.execute("""select f.title as TITLE from rental as r
        join inventory as i
        on r.inventory_id = i.inventory_id
        join film as f
        on i.film_id = f.film_id
        join film_actor as fa
        on fa.film_id = i.film_id
        where fa.actor_id = %s
        group by i.film_id
        order by count(r.rental_id) desc
        limit 5;""", (actorID,))
    response = jsonify(cursor.fetchall())
    return response

@app.route('/customers', methods=['GET'])
def get_cust():
    cursor.execute("""
        SELECT customer_id AS ID, first_name AS FIRST, last_name AS LAST, email AS EMAIL 
        FROM sakila.customer;
    """)
    return jsonify(cursor.fetchall())

@app.route('/customers/<int:id>', methods=['GET'])
def get_one_cust(id):
    cursor.execute("""
        SELECT customer_id AS ID, first_name AS FIRST, last_name AS LAST, email AS EMAIL 
        FROM sakila.customer WHERE customer_id = %s;
    """, (id,))
    result = cursor.fetchone()
    
    if result:
        return jsonify(result)  # Customer exists, return data
    else:
        return jsonify({"error": "Customer not found"}), 404  # Explicit 404 error




@app.route('/customers', methods=['POST'])
def add_customer():
    data = request.get_json()

    cursor.execute("""
        INSERT INTO customer (store_id, first_name, last_name, email, address_id) 
        VALUES (%s, %s, %s, %s, %s);
    """, (1, data['first_name'], data['last_name'], data['email'], 1))

    new_id = cursor.lastrowid  # Get auto-generated ID
    db.commit()
    
    return jsonify({"message": "Customer added successfully", "customer_id": new_id}), 201


@app.route('/customers/<int:id>', methods=['DELETE'])
def delete_customer(id):
    cursor.execute("DELETE FROM customer WHERE customer_id = %s;", (id,))
    db.commit()
    return jsonify({"message": "Customer deleted successfully"})

@app.route('/customers/<int:id>', methods=['PUT', 'PATCH'])
def update_customer(id):
    data = request.get_json()

    if request.method == 'PUT':  # Full update (All fields required)
        cursor.execute("""
            UPDATE customer 
            SET first_name=%s, last_name=%s, email=%s 
            WHERE customer_id=%s
        """, (data['first_name'], data['last_name'], data['email'], id))

    elif request.method == 'PATCH':  # Partial update
        updates = []
        values = []
        if 'first_name' in data:
            updates.append("first_name=%s")
            values.append(data['first_name'])
        if 'last_name' in data:
            updates.append("last_name=%s")
            values.append(data['last_name'])
        if 'email' in data:
            updates.append("email=%s")
            values.append(data['email'])

        if updates:  # Avoid running an empty SQL query
            query = f"UPDATE customer SET {', '.join(updates)} WHERE customer_id=%s"
            values.append(id)
            cursor.execute(query, values)

    db.commit()
    return jsonify({"message": "Customer updated successfully"})



if __name__ == '__main__':
    app.run(port=5000, debug=True)