xpath_key_value = ['Portfolio Assets Date', 'Turnover Rate Date', 'Total Net Assets', 'Portfolio Assets',
                   'Turnover Rate', 'Share Inception Date', 'Benchmark 1']


sql_query = """select * from public.xpath where (domain
like '%hennessyfunds%'or domain like '%blackcreekdiversified%'
or domain like '%bouldercef%' or domain like'%bridgebuildermutualfunds%'
or domain like '%calamos%' or domain like '%cambiar%' or domain like '%cloughglobal%'
or domain like '%fwcapitaladvisors%')
and fieldtype not in ('Key Value Pair','Minicrawl', 'Simple Text / Number','URL of PDF') and fieldname not like '%Entire%';"""