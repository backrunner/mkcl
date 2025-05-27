import yaml
import argparse
import time
import datetime
from core import clean_data
from chart import clean_chart

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', metavar='PATH', help='Path to misskey configuration file', action='store', default='.config/default.yml')
parser.add_argument('-d', '--days', metavar='DAYS', help='The days between now and the date when clear script stop cleaning', action='store', default=28, type=int)
parser.add_argument('-s', '--start_date', metavar='DATE', help='The start date for cleaning', action='store', default='2021-01-01')
parser.add_argument('-e', '--end_date', metavar='DATE', help='The end date for cleaning', action='store')
parser.add_argument('-w', '--weeks', metavar='WEEKS', help='Week Mode', action='store', type=int)
parser.add_argument('-m', '--months', metavar='MONTHS', help='30 days Mode', action='store', type=int)
parser.add_argument('-n', '--no_post', help='No Post Mode', action='store_true')
parser.add_argument('-chart','--chart',metavar='DAYS', help='Clean Chart days', action='store', type=int)
parser.add_argument('-chart_only','--chart_only',metavar='DAYS', help='Clean Chart days only', action='store', type=int)
parser.add_argument('-sfile','--single_file', help='Clean Single File Only', action='store_true')
parser.add_argument('-t', '--timeout', metavar='MINUTES', help='Total operation timeout in minutes (default: 180)', action='store', default=180, type=int)
parser.add_argument('--verbose', help='Enable verbose debug output', action='store_true')

args = parser.parse_args()

if args.end_date is not None:
    end_date_str = args.end_date
    start_date_str = args.start_date
elif args.weeks is not None:
    end_date = time.localtime(time.time() - 60*60*24*7*args.weeks)
    end_date_str = str(time.strftime('%Y-%m-%d', end_date))
    start_date = time.localtime(time.time() - 60*60*24*7*(args.weeks+1))
    start_date_str = str(time.strftime('%Y-%m-%d', start_date))
elif args.months is not None:
    end_date = time.localtime(time.time() - 60*60*24*30*args.months)
    end_date_str = str(time.strftime('%Y-%m-%d', end_date))
    start_date = time.localtime(time.time() - 60*60*24*30*(args.months+1))
    start_date_str = str(time.strftime('%Y-%m-%d', start_date))
else:
    end_date = time.localtime(time.time() - 60*60*24*args.days)
    end_date_str = str(time.strftime('%Y-%m-%d', end_date))
    start_date_str = args.start_date

with open(args.config) as config_file:
    config = yaml.load(config_file, Loader=yaml.FullLoader)

id_type = config["id"]

if id_type not in ["aid", "aidx"]:
    print("不支持的id类型")
    exit()

db_config = config['db']
redis_config = config['redis']
url = config['url']

if args.chart_only is not None:
    clean_chart([db_config['host'], db_config['port'], db_config['db'], db_config['user'], db_config['pass']], args.chart_only)
    result_message = '\n清理部分图表数据成功'
    print(result_message)
    exit(0)

start_time = datetime.datetime.now()
cleaning_result = clean_data([db_config['host'], db_config['port'], db_config['db'], db_config['user'], db_config['pass']], 
                             [redis_config['host'], redis_config['port'], redis_config.get('pass'), redis_config.get('db')], 
                             start_date_str, end_date_str, args.timeout, verbose=args.verbose)
end_time = datetime.datetime.now()
duration = end_time - start_time
result_message = '成功执行数据库清理\n清理范围:{}至{}\n{}\n用时{}s\n超时设置:{}分钟'.format(
    start_date_str, end_date_str, cleaning_result, duration.seconds, args.timeout)

if args.chart is not None:
    clean_chart([db_config['host'], db_config['port'], db_config['db'], db_config['user'], db_config['pass']], args.chart)
    result_message += '\n清理部分图表数据成功'

print(result_message)
