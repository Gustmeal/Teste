from flask import Blueprint, render_template, redirect, url_for
from flask_login import login_required, current_user
from datetime import datetime

main_bp = Blueprint('main', __name__)

@main_bp.context_processor
def inject_current_year():
    return {'current_year': datetime.utcnow().year}

@main_bp.route('/geinc')
@login_required
def geinc_index():
    return render_template('geinc/index.html')