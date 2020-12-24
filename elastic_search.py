from elasticsearch import Elasticsearch
from beautifultable import BeautifulTable
import sys
import os
import json

class ESearch:
    def __init__(self):
        self.es = Elasticsearch()

    # функция создания индекса
    def create_index(self):
        # es.indices -> доступ к IndicesClient, и в нем .create
        indexItem = 'film'
        bodyItem = {
            'settings': {
                # осколки
                'number_of_shards': 1,
                # реплики
                'number_of_replicas': 0,
                # настраиваем анализатор
                'analysis': {
                    'filter': {
                        'ru_stop': {
                                'type': 'stop',
                                'stopwords': '_russian_'
                            },
                        'ru_stemmer': {
                                'type': 'stemmer',
                                'language': 'russian'
                            }
                        },
                    'analyzer': {
                        'default': {
                                'tokenizer': 'standart',
                                'filter': ['snowball', 'lowercase', 'worlddelimiter', 'ru_stop', 'ru_stemmer']
                            }
                        }
                    }
                }
            }
        self.es.indices.create(index = indexItem, body = bodyItem, ignore = 400)
    
    def delete_index(self):
        # .get_alias() - возвращает информацию об одном или нескольких копиях индекса
        for key in self.es.indices.get_alias().keys():
            self.es.indices.delete(index = key)

    def add_index(self):
        if (len(sys.argv) > 2 or len(sys.argv) < 2):
            print('Введите команду для выполнения: python elastic_search.py путь_к_файлу_output.json')
            sys.exit(-1)
        path = os.path.abspath(sys.argv[1])
        with open(path, 'r') as file_stream:
            data = json.loads(file_stream.read())
            i = 0
            for item in data:
                self.es.index(index = 'film', id = i, body = item)
                i += 1

    def find_response(self, option, request):
        fields_list = [['title'], ['body'], ['title', 'body']]
        field = fields_list[int(option)]

        bodyItem = {
            # настройка поискового запроса
            'query': {
                'bool': {
                    'should': [
                        {
                            'multi_match': {
                                'query': request,
                                'analyzer': 'default',
                                'fields': field
                            }
                        },
                    ],
                }
            }
        }

        return self.es.search(index = 'film', body = bodyItem)

if __name__ == '__main__':
    search_index = ESearch()
    search_index.delete_index()
    print('Создание индекса')
    search_index.create_index()
    print('Индекс создался')
    print('Добавление к индексу данных')
    search_index.add_index()
    print('Добавили к индексу данные')

    option = input('Нажмите, чтобы выполнить поиск по:\n0 - заголовку,\n1 - тексту\n2 - тексту и заголовку\n')
    if option in ['0', '1', '2']:
        request = input('Введите поисковой запрос: ')
        result = search_index.find_response(option, request)
        result_len = len(result['hits']['hits'])
        if result_len > 0:
            table = BeautifulTable()
            for i in range(result_len):
                table.rows.append([i + 1, result['hits']['hits'][i]['_source']['url'], result['hits']['hits'][i]['_score'], result['hits']['hits'][i]['_source']['title']])
            table.columns.header = ['Порядок', 'URL', 'Score', 'Заголовок']
            print(table)
    else:
        print('Ошибка: нераспознанный символ')
