# coding=UTF-8
from flask import g
from webhelpers.text import truncate

def static(link):
    return g.config.get('static_base', '/static/') + link
