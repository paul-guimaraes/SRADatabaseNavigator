from django.urls import path
from . import views

appname = 'sra'
urlpatterns = [
    path('', views.index, name='index'),
    path('add_table_join_fields/', views.add_table_join_fields, name='add_table_join_fields'),
    path('download_job/<str:result>/', views.download_job, name='download_job'),
    path('get_network/', views.get_network, name='get_network'),
    path('get_possible_equals_columns/', views.get_possible_equals_columns, name='get_possible_equals_columns'),
    path('load_screen/', views.load_screen, name='load_screen'),
    path('reject_table_join_fields/', views.reject_table_join_fields, name='reject_table_join_fields'),
    path('search_data/', views.search_data, name='search_data'),
]
