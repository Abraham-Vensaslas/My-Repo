from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_login import LoginManager
from flask_mail import Mail



app = Flask(__name__)
app.config['SECRET_KEY'] = 'd8783c86dd2fadccb9d91118b307a4f6'
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///site.db'
db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
login_manager = LoginManager(app)
login_manager.login_view= 'login'
login_manager.login_message_category = 'info'

app.config['MAIL_SERVER'] = 'smtp.yandex.com'
app.config['MAIL_PORT'] = 465
app.config['MAIL_USE_TLS'] = True
app.config['MAIL_USERNAME'] = 'writerden@yandex.com'
app.config['MAIL_PASSWORD'] = 'Instawriter'
mail=Mail(app)

from flaskblog import routes
