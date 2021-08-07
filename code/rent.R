  source(
    'code/RPackages.R',
    encoding = 'UTF-8'
  )
  


# 解壓縮歷史資料 -----------------------------------------------------------------

  # 先解單一個檔案
    system2(
      "C:/Program Files/7-Zip/7z.exe",
      args = 'x C:/R_work/rent/data/lvr_landxml_110q1.zip -oC:/R_work/rent/data/lvr_landxml_110q1'
    )
  
  system2(
    "C:/Program Files/7-Zip/7z.exe",
    args = 'x C:/R_work/rent/data/lvr_landxml_110q1.zip -oC:/R_work/rent/data/lvr_landxml_110q1'
  )
  
  unlink('data/lvr_landxml_110q1',
         recursive = T)
    
  file.remove('data/lvr_landxml_110q1')
  unlink('data/lvr_landxml_110q1',
         recursive = T)
  
  # 以迴圈批次解壓縮
    for(file_name in dir('data/lvr_land_history', 
                         pattern = '.zip$')){
      system2("C:/Program Files/7-Zip/7z.exe",
              args = paste0("x ", 
                            getwd(), '/data/lvr_land_history/', 
                            file_name, " -o", 
                            getwd(), 
                            '/data/lvr_land_history/data_history/',
                            str_replace_all(
                              file_name,
                              pattern = '.zip',
                              replacement = '')
              )
      )
    }




# 合併 107~110 全國租賃(土地+建物)資料-----------------------------------------------------------------
  root_path <- 
    paste(sep = '/',
          getwd(),
          'data/lvr_land_history/data_history')
  
  file_folder <- 
    dir(root_path)
  
  tmp <- 
    map_dfr(
      file_folder,
      function(folder){
        data_file <- 
          dir(paste(sep = '/',
                    root_path,
                    folder),
              pattern = '_lvr_land_c.csv')
        
        tmp <- 
          map_dfr(
            data_file,
            function(file_name){
              
              raw_data <- 
                read.csv(
                  file = paste(sep = '/',
                               root_path,
                               folder,
                               file_name),
                  fileEncoding = 'UTF-8-BOM',
                  skip = 1,
                  colClasses = c(building.shifting.total.area = 'double',
                                 land.shifting.total.area.square.meter = 'character',
                                 transaction.year.month.and.day = 'character')
                )
              
              # var_name <- 
              #   fread(
              #     file = paste(sep = '/',
              #                  root_path,
              #                  folder,
              #                  file_name),
              #     encoding = 'UTF-8',
              #     nrows = 1
              #   ) %>% 
              #   names()
              # 
              # names(raw_data) <- var_name
              
              raw_data <- 
                raw_data %>% 
                mutate(
                  data_source = file_name
                ) 
              
              return(raw_data)
            }
          )
        return(tmp)
      }
    )
  
  rent_result <- 
    left_join(
      tmp,
      fread(
        file = paste(sep = '/',
                     root_path,
                     file_folder[1],
                     'manifest.csv'),
        encoding = 'UTF-8',
        select = c('name', 'description')
      ),
      by = c('data_source' = 'name')
    ) %>% 
    mutate(
      city = str_sub(description, 1, 3)
    ) %>% 
    filter(grepl(serial.number,
                  pattern = '^[A-Z]')) %>% 
    mutate(
      land.shifting.total.area.square.meter = as.double(land.shifting.total.area.square.meter),
      transaction.year.month.and.day = as.integer(transaction.year.month.and.day)
    )
  
  write_excel_csv(
    rent_result %>% 
      filter(grepl(city, pattern = '北市')),
    file = paste(sep = '/',
                 getwd(),
                 'data/result',
                 'RVRS_107.csv'),
    na = ''
  )


# 產生完整住址清單 ----------------------------------------------------------------
  # 地號為預售屋
  taipei <- 
    fread(
      file = paste(sep = '/',
                   getwd(),
                   'data/result',
                   'RVRS_107_110q1.csv'),
      encoding = 'UTF-8'
    )
  
  address_list <- 
    distinct(taipei[!grepl(land.sector.position.building.sector.house.number.plate,
                          pattern = '(地號$)') &
                    grepl(land.sector.position.building.sector.house.number.plate,
                           pattern = '^[新臺台]北市') &
                    !grepl(land.sector.position.building.sector.house.number.plate,
                           pattern = '([0-9]{5,10}~[0-9]{5,10})|(新北市淡水區民權路號|臺北市中山區長春路號|新北市三重區成功路104巷號)'),] , 
             land.sector.position.building.sector.house.number.plate) %>% 
    rename(
      address = land.sector.position.building.sector.house.number.plate
    )
  
  tmp <- 
    address_list %>% 
    mutate(
      start = 
        str_extract(address, pattern = '[0-9]{1,4}~') %>% 
        str_replace_all(pattern = '~', 
                        replacement = ''),
      end = 
        str_extract(address, pattern = '~[0-9]{1,4}號') %>% 
        str_replace_all(pattern = '(~|號)', 
                        replacement = ''),
      prefix_address = 
        str_replace_all(
          address,
          pattern = '[0-9]{1,4}~[0-9]{1,4}號',
          replacement = '')
    ) %>% 
    mutate(
      prefix_address = 
        case_when(
          grepl(prefix_address, 
                pattern = '((臺北|新北)市.*區){2}') ~
            paste0(
              str_split(prefix_address,
                        pattern = '區',
                        simplify = T)[,1],
              '區',
              str_replace_all(
                prefix_address,
                pattern = '((臺北|新北)市.*區){2}',
                replacement = ''
              )
            ),
          T ~ prefix_address
        )
    ) %>% 
    distinct()
  
ptm <- proc.time()
  address_result <- 
    map_dfr(
      tmp$prefix_address[1:dim(tmp)[1]],
      function(x){
        map_dfr(
          tmp[prefix_address==x,]$start:tmp[prefix_address==x,]$end,
          function(seq_num){
            data.table(
              Address = paste0(x, seq_num, '號'),
              Response_Address = '',
              Response_X = '',
              Response_Y = ''
            )
          }
        )
      }
    ) %>% 
    distinct() %>% 
    mutate(id = row_number()) %>% 
    select(id, Address, Response_Address, 
           Response_X, Response_Y)
proc.time() - ptm

  map(
    seq(from = 1, to = dim(address_result)[1], by = 10000),
    function(x){
      write.csv(
        address_result[x:(x+9999),],
        file = paste0('data/result/address_list/address_detail_', 
                      x %/% 10000, 
                      '.csv'),
        fileEncoding = 'Big5',
        row.names = F
      ) 
    }
  )
  
  for(i in seq(from = 1, to = dim(address_result)[1], by = 10000)){
    write_excel_csv(
      address_result[i:(i+9999),],
      file = 'data/result/address_list/'
    )
  }
  
# 合併資料 --------------------------------------------------------------------

  file_folder <- 
    paste(sep = '/',
          'data/lvr_land_history/data_history',
          'lvr_landxml_109q4')
  
  data_file <- 
    dir(file_folder,
        pattern = '_lvr_land_c.csv')
  
  # 2, 13, 14, 18-lvr_landxml_109q3
  # 1, 2, 5, 6, 15
  check_data_1 <- 
    map_dfr(
      data_file,
      function(x){
        inner_tmp <- 
          read.csv(
            file = paste(sep = '/',
                         file_folder,
                         x),
            fileEncoding = 'UTF-8-BOM',
            colClasses = c(building.shifting.total.area = 'double',
                           land.shifting.total.area.square.meter = 'character'),
            skip = 1,
          )
        
        # var_name <- 
        #   fread(
        #     file = paste(sep = '/',
        #                  file_folder,
        #                  x),
        #     encoding = 'UTF-8',
        #     nrows = 1
        #   ) %>% 
        #   names()
        # 
        # names(inner_tmp) <- var_name
        
        inner_tmp <- 
          inner_tmp %>% 
          mutate(
            data_source = x
          )
      }
    )
  
  check_data_2 <- 
    map_dfr(
      data_file[15],
      function(x){
        inner_tmp <- 
          read.csv(
            file = paste(sep = '/',
                         file_folder,
                         x),
            fileEncoding = 'UTF-8-BOM',
            skip = 1
          )
        
        # var_name <- 
        #   fread(
        #     file = paste(sep = '/',
        #                  file_folder,
        #                  x),
        #     encoding = 'UTF-8',
        #     nrows = 1
        #   ) %>% 
        #   names()
        # 
        # names(inner_tmp) <- var_name
        
        inner_tmp <- 
          inner_tmp %>% 
          mutate(
            data_source = x
          )
      }
    )
  
  check_data_schema()
  
  check_data_schema <- 
    function(){
      str(check_data_1[12,])
      str(check_data_2[12,])
    }
  check_data_schema()
  
  
  concatenate_by_key_word <- 
    function(key_word){
      data_file <- 
        dir(file_folder,
            pattern = paste0('_', 
                             key_word,
                             '.csv$'))
    
      
      inner_tmp <- 
        read.csv(
          file = paste(sep = '/',
                       file_folder,
                       'a_lvr_land_a.csv'),
          fileEncoding = 'UTF-8-BOM',
          skip = 1
        )
      
      var_name <- 
        fread(
          file = paste(sep = '/',
                       file_folder,
                       'a_lvr_land_a.csv'),
          nrows = 1
        ) %>% 
        names()
      
      names(inner_tmp) <- var_name
      
      inner_tmp <- 
        inner_tmp %>% 
        mutate(
          data_source = x
        )
      
      
      tmp <- 
        map_dfr(
          data_file,
          function(x){
            inner_tmp <- 
              fread(
                file = paste(sep = '/',
                             file_folder,
                             x),
                encoding = 'UTF-8',
                skip = 1
              ) 
            names(inner_tmp) <- 
              fread(
                file = paste(sep = '/',
                             file_folder,
                             x),
                encoding = 'UTF-8',
                nrows = 1
              ) %>% 
              names()
            
            inner_tmp <- 
              inner_tmp %>% 
              mutate(
                data_source = x
              )
            return(inner_tmp)
          }
        )
      
      return(tmp)
    }
  
  
  lrv_land_a <- 
    concatenate_by_key_word('a')
  lvr_land_c <- 
    concatenate_by_key_word('c')
  lvr_land_b <- 
    concatenate_by_key_word('b')
  lvr_land_build <- 
    concatenate_by_key_word('build')
  lvr_land_land <- 
    concatenate_by_key_word('land')
  lvr_land_history <- 
    concatenate_by_key_word('history')
  