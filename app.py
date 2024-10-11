from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import distinct
import config

app = Flask(__name__)
app.config.from_object(config)
db = SQLAlchemy(app)

class Users(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    end_year = db.Column(db.String(100))
    intensity = db.Column(db.String(100))
    sector = db.Column(db.String(100))
    topic = db.Column(db.String(100))
    insight = db.Column(db.String(100))
    url = db.Column(db.String(100))
    region = db.Column(db.String(100))
    start_year = db.Column(db.String(100))
    impact = db.Column(db.String(100))
    added = db.Column(db.String(100))
    published = db.Column(db.String(100))
    country = db.Column(db.String(100))
    relevance = db.Column(db.Integer)
    pestle = db.Column(db.String(100))
    source = db.Column(db.String(100))  # Source column
    title = db.Column(db.String(100))
    likelihood = db.Column(db.Integer)

    def __repr__(self):
        return f"<User {self.title}>"

# Add Users
@app.route("/add_user", methods=['POST'])
def add_users():
    data = request.json
    if not isinstance(data, list):
        return jsonify({"message": "Invalid data format, expected an array of objects"}), 400
    try:
        # Loop through each user object in the array and create User instances
        for user_data in data:
            new_user = Users(
                end_year=user_data.get('end_year'),
                intensity=user_data.get('intensity'),
                sector=user_data.get('sector'),
                topic=user_data.get('topic'),
                insight=user_data.get('insight'),
                url=user_data.get('url'),
                region=user_data.get('region'),
                start_year=user_data.get('start_year'),
                impact=user_data.get('impact'),
                added=user_data.get('added'),
                published=user_data.get('published'),
                country=user_data.get('country'),
                relevance=user_data.get('relevance'),
                pestle=user_data.get('pestle'),
                source=user_data.get('source'),
                title=user_data.get('title'),
                likelihood=user_data.get('likelihood')
            )
            db.session.add(new_user)

        db.session.commit()
        return jsonify({"message": "Users added successfully"}), 201

    except Exception as e:
        db.session.rollback()
        return jsonify({"message": "Error occurred", "error": str(e)}), 500

# Get All Users
@app.route("/users", methods=['GET'])
def get_users():
    try:
        users = Users.query.all()
        user_list = [
            {
                'id': user.id,
                'end_year': user.end_year,
                'intensity': user.intensity,
                "sector": user.sector,
                "topic": user.topic,
                "insight": user.insight,
                "url": user.url,
                "region": user.region,
                "start_year": user.start_year,
                "impact": user.impact,
                "added": user.added,
                "published": user.published,
                "country": user.country,
                "relevance": user.relevance,
                "pestle": user.pestle,
                "source": user.source,
                "title": user.title,
                "likelihood": user.likelihood
            } for user in users
        ]
        return jsonify(user_list), 200
    except Exception as e:
        return jsonify({"message": "Error occurred", "error": str(e)}), 500

# # 1. Route to fetch column data
# @app.route("/get-column", methods=['GET'])
# def get_column_data():
#     column_name = request.args.get('column', None)  # Get column name from query parameter

#     if not column_name:
#         return jsonify({"error": "No column name provided"}), 400

#     # Check if the column exists in the Users model
#     if not hasattr(Users, column_name):
#         return jsonify({"error": f"Column '{column_name}' does not exist"}), 400

#     # Fetch all values for that column
#     column_data = db.session.query(getattr(Users, column_name)).all()
#     values = [item[0] for item in column_data]  # Unwrap tuple result from query

#     return jsonify({column_name: values})


@app.route("/get-column", methods=['GET'])
def get_column_data():
    column_name = request.args.get('column', None)  # Get column name from query parameter

    if not column_name:
        return jsonify({"error": "No column name provided"}), 400

    # Check if the column exists in the Users model
    if not hasattr(Users, column_name):
        return jsonify({"error": f"Column '{column_name}' does not exist"}), 400

    try:
        # Fetch distinct values for the column
        column_data = db.session.query(distinct(getattr(Users, column_name))).all()
        values = [item[0] for item in column_data]  # Unwrap tuple result from query

        return jsonify({column_name: values})

    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

# 2. Route to filter table based on column value
@app.route("/filter-column", methods=['GET'])
def filter_column_by_value():
    column_name = request.args.get('column', None)
    value = request.args.get('value', None)

    if not column_name or not value:
        return jsonify({"error": "Column name or value not provided"}), 400

    # Check if the column exists in the Users model
    if not hasattr(Users, column_name):
        return jsonify({"error": f"Column '{column_name}' does not exist"}), 400

    # Filter the database based on column name and value
    column_attr = getattr(Users, column_name)
    filtered_users = Users.query.filter(column_attr == value).all()

    # Convert the result into a list of dictionaries
    result = [
        {
            'id': user.id,
            'end_year': user.end_year,
            'intensity': user.intensity,
            "sector": user.sector,
            "topic": user.topic,
            "insight": user.insight,
            "url": user.url,
            "region": user.region,
            "start_year": user.start_year,
            "impact": user.impact,
            "added": user.added,
            "published": user.published,
            "country": user.country,
            "relevance": user.relevance,
            "pestle": user.pestle,
            "source": user.source,
            "title": user.title,
            "likelihood": user.likelihood
        } for user in filtered_users
    ]

    return jsonify(result)

# Main app entry point
if __name__ == '__main__':
    app.run(debug=True)
