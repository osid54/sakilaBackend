from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
from datetime import datetime
import mysql.connector

app = Flask(__name__)
CORS(app)

db = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="pass",
    database="sakila"
)
cursor = db.cursor(dictionary=True)

@app.route('/films', methods=['GET'])
def get_films():
    cursor.execute("select * from sakila.film_list;")
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
    actor_id = request.args.get('id')
    cursor.execute("""select f.title as title from rental as r
        join inventory as i
        on r.inventory_id = i.inventory_id
        join film as f
        on i.film_id = f.film_id
        join film_actor as fa
        on fa.film_id = i.film_id
        where fa.actor_id = %s
        group by i.film_id
        order by count(r.rental_id) desc
        limit 5;""", (actor_id,))
    response = jsonify(cursor.fetchall())
    return response

@app.route('/customers', methods=['GET'])
def get_cust():
    cursor.execute("""
        select customer_id as ID, first_name as "FIRST", last_name as "LAST", email as EMAIL from sakila.customer;;
    """)
    return jsonify(cursor.fetchall())

@app.route('/customers/<int:id>', methods=['GET'])
def get_one_cust(id):
    cursor.execute("""
        select customer_id as id, first_name as "FIRST", last_name as "LAST", email as EMAIL 
        from sakila.customer where customer_id = %s;
    """, (id,))
    result = cursor.fetchone()
    
    if result:
        return jsonify(result)
    else:
        return jsonify({"error": "Customer not found"}), 404

@app.route('/customers', methods=['POST'])
def add_customer():
    data = request.get_json()
    
    cursor.execute("""
        insert into customer (store_id, first_name, last_name, email, address_id) 
        values (%s, %s, %s, %s, %s);
    """, (1, data['first_name'], data['last_name'], data['email'], 1))
    
    new_id = cursor.lastrowid
    db.commit()
    
    return jsonify({"message": "Customer added successfully", "customer_id": new_id}), 201

@app.route('/customers/<int:id>', methods=['DELETE'])
def delete_customer(id):
    cursor.execute("delete from customer where customer_id = %s;", (id,))
    db.commit()
    return jsonify({"message": "Customer deleted successfully"}), 200

@app.route('/customers/<int:id>', methods=['PUT', 'PATCH'])
def update_customer(id):
    data = request.get_json()
    
    if request.method == 'PUT': 
        cursor.execute("""
            update customer 
            set first_name=%s, last_name=%s, email=%s 
            where customer_id=%s
        """, (data['first_name'], data['last_name'], data['email'], id))
    
    elif request.method == 'PATCH':  
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
        
        if updates:  
            query = f"update customer set {', '.join(updates)} where customer_id=%s"
            values.append(id)
            cursor.execute(query, values)
    
    db.commit()
    return jsonify({"message": "Customer updated successfully"}), 200

@app.route('/customers/<int:customer_id>/rentals', methods=['GET'])
def get_rental_history(customer_id):
    cursor.execute("""
        select rental_id, rental_date, return_date 
        from rental 
        where customer_id = %s;
    """, (customer_id,))
    rentals = cursor.fetchall()
    return jsonify(rentals)

@app.route('/rentals/<int:rental_id>/return', methods=['PATCH'])
def mark_rental_returned(rental_id):
    cursor.execute("""
        update rental
        set return_date = now()
        where rental_id = %s and return_date is null;
    """, (rental_id,))
    db.commit()
    return jsonify({"message": "Rental marked as returned"}), 200

@app.route('/rentals', methods=['POST'])
def rent_film():
    data = request.json
    customer_id = data.get("customer_id")
    film_id = data.get("film_id")
    staff_id = 1 

    if not customer_id or not film_id:
        return jsonify({"error": "Missing customer_id or film_id"}), 400

    cursor.execute("""
        select inventory_id from inventory 
        where film_id = %s 
        and inventory_id not in (select inventory_id from rental where return_date is null)
        limit 1;
    """, (film_id,))
    
    inventory = cursor.fetchone()
    
    if not inventory:
        return jsonify({"error": "No copies available for rental"}), 400

    inventory_id = inventory["inventory_id"]
    rental_date = datetime.now()

    cursor.execute("""
        insert into rental (rental_date, inventory_id, customer_id, staff_id)
        values (%s, %s, %s, %s);
    """, (rental_date, inventory_id, customer_id, staff_id))
    db.commit()

    return jsonify({"message": "Rental successful", "rental_id": cursor.lastrowid})

if __name__ == '__main__':
    app.run(port=5000, debug=True)
