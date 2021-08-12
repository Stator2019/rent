  source(
    'code/RPackages.R',
    encoding = 'UTF-8'
  )


# 解壓縮歷史資料 -----------------------------------------------------------------

  # 先解單一個檔案
  # 介紹command line。先用 system2 解壓縮單一個檔案，
  # 接著用 unlink 刪除解壓縮出來的資料夾。
  # 再來以迴圈批次解壓縮。批次解壓縮後，
  # 稍微看一下資料夾內容。(10~15分鐘)
  
  system2(
    "C:/Program Files/7-Zip/7z.exe",
    args = 'x C:/R_work/rent/data/lvr_landxml_110q1.zip -oC:/R_work/rent/data/lvr_landxml_110q1'
  )
  
  unlink('data/lvr_landxml_110q1',
         recursive = T)
    
  
  
  # 以迴圈批次解壓縮(5~10分鐘)
    for(file_name in dir('data', 
                         pattern = '.zip$')){
      system2("C:/Program Files/7-Zip/7z.exe",
              args = paste0("x ", 
                            getwd(), '/data/', 
                            file_name, " -o", 
                            getwd(), 
                            '/data/lvr_land_history/',
                            str_replace_all(
                              file_name,
                              pattern = '.zip',
                              replacement = '')
              )
      )
    }
 

# 合併全國租賃(土地+建物)資料-----------------------------------------------------------------
  
  root_path <- 
    paste(sep = '/',
          getwd(),
          'data/lvr_land_history')
  
  # 先合併一季
  
  file_folder <- 
    paste(sep = '/',
          root_path,
          'lvr_landxml_110q1')
  
  data_file <- 
    dir(file_folder,
        pattern = '_lvr_land_c.csv')
  
  # 合併時先直接用 UTF-8 合併，然後會發現有問題。以VS code 開啟原始資料後
  # 會發現帶有 UTF-8-BOM。再引導學生在 HELP 中查到 UTF-8-BOM 的資訊
  # 這裡可以順便提 RStudio 與說明文件整合很好的好處(10~15分鐘)
  
  tmp <- 
    map_dfr(
      data_file,
      function(x){
        raw_data <- 
          read.csv(
            file = paste(sep = '/',
                         file_folder,
                         x),
            fileEncoding = 'UTF-8-BOM',
            header = F,
            skip = 2
          )
        
        var_name <- 
          read.csv(
            file = paste(sep = '/',
                         file_folder,
                         x),
            fileEncoding = 'UTF-8-BOM',
            header = T,
            nrows = 1
          )
        
        names(raw_data) <- names(var_name)
        
        raw_data <- 
          raw_data %>% 
            mutate(
              data_source = str_replace_all(x, pattern = '.csv', '')
            )
        
        return(raw_data)
      }
    )
  
  # 一次合併全部
  # 實務小技巧：產生「資料資料夾」、「資料名稱」以及「流水號」
  # 直接合併後，發現「土地面積平方公尺」及「租賃年月日」的型態不一致(15~20分鐘)
  
  file_folder <- 
    dir(root_path)
  
  tmp_folder <- 
    map_dfr(
      file_folder,
      function(folder){
        map_dfr(
          dir(paste(sep = '/',
                    root_path,
                    folder), 
              pattern = '_lvr_land_c.csv'),
          function(x){
            raw_data <- 
              read.csv(
                file = paste(sep = '/',
                             root_path,
                             folder,
                             x),
                fileEncoding = 'UTF-8-BOM',
                header = F,
                skip = 2
              )
            
            var_name <- 
              read.csv(
                file = paste(sep = '/',
                             root_path,
                             folder,
                             x),
                fileEncoding = 'UTF-8-BOM',
                header = T,
                nrows = 1
              )
            
            names(raw_data) <- names(var_name)
            
            raw_data <- 
              raw_data %>% 
              mutate(
                data_folder = folder,
                data_source = x,
                土地面積平方公尺 = as.character(土地面積平方公尺),
                租賃年月日 = as.character(租賃年月日)
              )
            
            return(raw_data)
          }
        )
      }
    ) %>% 
    mutate(
      seq_id = row_number()
    )
  
  # 合併之後簡單地檢查資料
  str(tmp_folder)
  distinct(tmp_folder, 租賃年月日) %>% 
    arrange(租賃年月日) %>% 
    View()
  
  # 發現租賃年月日裡有英文字。打開原始資料發現是因","造成資料偏移
  # 解決因","造成資料偏移的問題(15~20分鐘)
  
  correct_text <- 
    str_c(tmp_folder[16898, 1:7],
          collapse = '|')
  
  tmp_folder <- 
    bind_rows(
      tmp_folder[-(16897:16898),],
      tmp_folder[16897, ] %>% 
        mutate(備註 = paste(sep = '|', 備註, correct_text),
                 編號 = tmp_folder[16898, 8])
    )
  
  # 與 mainifest.csv 串接，得知檔名意義
  # 在串接之前，務必確保資料內是一對一，避免之後串資料時有資料澎脹的問題
  # (10~15分鐘)
  
  manifest <- 
    map_dfr(
      file_folder,
      function(x){
        read.csv(
          file = paste(sep = '/',
                       root_path,
                       x,
                       'manifest.csv'),
          fileEncoding = 'UTF-8-BOM'
        )
      }
    ) %>% 
    distinct(name, description)
  
  rent_result <- 
    left_join(
      tmp_folder,
      manifest %>% 
        filter(grepl(name, pattern = 'lvr_land_c.csv')) %>% 
        distinct(),
      by = c('data_source' = 'name')
    ) %>% 
    mutate(
      縣市 = str_sub(description, 1, 3)
    )
  
  # 處理時間-將建築剛成年月轉成日期 yyyymmdd 格式(20 ~ 25)
  
  write_excel_csv(
    rent_result %>% 
      filter(grepl(city, pattern = '北市')),
    file = paste(sep = '/',
                 getwd(),
                 'data/result',
                 'RVRS_107.csv'),
    na = ''
  )
  
  # git 網址
  # https://github.com/Stator2019/rent.git


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
  