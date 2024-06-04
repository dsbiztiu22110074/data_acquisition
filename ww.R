# 演習課題
DB  <- 'weather_ex.duckdb'

# 気象観測所
site <- data.frame(
  id   = 47662,   # 番号
  name = 'Tokyo') # 名称（データベースのテーブル名として使う）


t.fr <- as.POSIXlt('2021-12-30')
t.to <- as.POSIXlt('2022-01-01')
ts   <- as.POSIXlt(seq(t.fr, t.to, by = 'days'))

library(duckdb)
library(rvest)

con <- dbConnect(duckdb(), DB)

for(i in 1:3)
{#i<-1
  year <- 1900 + ts[i]$year
  month <- 1 + ts[i]$mon
  day <- ts[i]$mday
  
  url <- paste0('https://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?prec_no=44&block_no=', site$id, '&year=', year, '&month=', month, '&day=', day, '&view=')
  cat('URL:', url, fill = T)
  
  
  read_html(url) |> html_table() -> tbl
  tbl
  
  d0 <- as.data.frame(tbl[[5]])
  str(d0)
  
  hour <- d0[-1, '時']
  
  datetime <- as.POSIXlt(paste(ts[i], hour)) 
  
  d1 <- data.frame(site.id   = as.integer(site$id), # 整数型
                   site.name = site$name,
                   datetime  = paste(datetime),
                   temp      = as.double(d0[-1, 5]), # 倍精度浮動小数点型
                   wind      = d0[-1, 10])
  str(d1)
  
  
  dbWriteTable(con, site$name, d1, append = T)
  Sys.sleep(runif(1, min = 1, max = 2))
}

res <- dbSendQuery(con, 'SELECT * FROM Tokyo')

dbFetch(res)

