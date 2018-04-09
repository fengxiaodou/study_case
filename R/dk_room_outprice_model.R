source("../header.R")
#source("R/header.R")
source("R/get_data_sql.R")


######dk标准间价格预估

std_room_outprice_pre_model<-xgb.load("Model/std_room_outprice_pre_model.model")

dk_std_room_convert_model_into_data2 <- mutate(dk_std_room_convert_model_into_data, 
                                               area_xy_dev = living_area_y - living_area_x, 
                                               face_xy_dev = face_y - face_x, 
                                               has_toilet_xy_dev = has_toilet_y - has_toilet_x, 
                                               has_balcony_xy_dev = has_balcony_y - has_balcony_x, 
                                               bedroom_xy_dev = bedroom_num_y - bedroom_num_x
                                               ,before_bedroom_xy_dev = before_room_num_y - before_room_num_x
                                               )

dk_forcast_colnams <- c('before_room_num_x', 
                        'bedroom_num_x', 'toilet_num_x', 'dw_num_x', 'xianfang_days_x', 
                        'price_x', 'face_x', 'has_toilet_x',
                        'living_area_x', 'has_balcony_x',
                        'before_room_num_y', 
                        'bedroom_num_y', 'toilet_num_y', 'dw_num_y', 'xianfang_days_y', 
                        'face_y', 'has_toilet_y',
                        'living_area_y', 'has_balcony_y',
                        'city_level', 
                        'area_xy_dev', 'face_xy_dev', 'bedroom_xy_dev', 
                        'has_toilet_xy_dev', 
                        'before_bedroom_xy_dev',
                        'has_balcony_xy_dev')


real_dk_std_room_convert_model_into_data2 <- select(dk_std_room_convert_model_into_data2, dk_forcast_colnams) %>% 
  sapply(as.numeric) 

dk_data <- dk_std_room_convert_model_into_data %>% 
  mutate(std_room_outprice_pre = price_x + predict(std_room_outprice_pre_model,real_dk_std_room_convert_model_into_data2)) %>% 
  group_by(city,block,community) %>%
  mutate(final_std_room_outprice_pre = quantile(std_room_outprice_pre, 0.5),
         community_rooms = n()) %>% 
  filter(community_rooms > 1)

openxlsx::write.xlsx(dk_data,paste("Results/",Sys.Date(),"dk小区间均价.xlsx"),row.names = FALSE)



 
#####ziroom标准间价格预估

zir_std_room_outprice_pre_model <- xgb.load("Model/zir_std_room_outprice_pre_model.model")
ziroom_std_room_convert_model_into_data2 <- mutate(ziroom_std_room_convert_model_into_data, 
                                                   area_xy_dev = area_y - area_x, 
                                                   face_xy_dev = face_y - face_x, 
                                                   bathroom_xy_dev = bathroom_y - bathroom_x, 
                                                   style_xy_dev = style_y - style_x, 
                                                   balcony_xy_dev = balcony_y - balcony_x, 
                                                   bedroomnum_xy_dev = bedroomnum_y - bedroomnum_x )

ziroom_forcast_colnams <- c("area_y","bathroom_y","face_y","balcony_y","style_y","bedroomnum_y",
                            "area_x","bathroom_x","face_x","balcony_x","style_x","bedroomnum_x","price_x",
                            "city_level","area_xy_dev","face_xy_dev","bathroom_xy_dev","balcony_xy_dev","style_xy_dev","bedroomnum_xy_dev")



real_ziroom_std_room_convert_model_into_data2 <- select(ziroom_std_room_convert_model_into_data2,ziroom_forcast_colnams)  %>% 
  sapply(as.numeric) 
ziroom_data <- ziroom_std_room_convert_model_into_data %>% 
  mutate(std_room_outprice_pre = price_x + predict(zir_std_room_outprice_pre_model,real_ziroom_std_room_convert_model_into_data2)) %>% 
  group_by(city,community) %>%
  mutate(final_std_room_outprice_pre = ifelse(city == '北京', quantile(std_room_outprice_pre, 0.75), quantile(std_room_outprice_pre, 0.6)),
         community_rooms = n()) %>% 
  filter(community_rooms > 1)

openxlsx::write.xlsx(ziroom_data,paste("Results/",Sys.Date(),"ziroom小区间均价.xlsx"),row.names = FALSE)


#######小区间均价处理

xiaoqu_std_price <- full_join(dk_data,ziroom_data, 
                              by =c("city", "community"), copy = FALSE, suffix = c(".dk_data", ".ziroom_data")) %>%  
  select(city, block.dk_data, community, final_std_room_outprice_pre.dk_data, final_std_room_outprice_pre.ziroom_data) %>% 
  distinct() %>%
  group_by(city, block.dk_data) %>%
  mutate(block_price.danke = mean(final_std_room_outprice_pre.dk_data, na.rm = TRUE),block_price.ziroom=mean(final_std_room_outprice_pre.ziroom_data, na.rm = TRUE)) %>% 
  mutate(xiaoqu_std_price_final = ifelse(is.na(final_std_room_outprice_pre.dk_data),final_std_room_outprice_pre.ziroom_data,final_std_room_outprice_pre.dk_data),
         block_std_price=ifelse(block_price.danke == 'NaN',block_price.ziroom,block_price.danke)) %>% 
           select(city, block.dk_data, community,xiaoqu_std_price_final,block_std_price) %>% 
           distinct()
    
openxlsx::write.xlsx(xiaoqu_std_price,paste("Results/",Sys.Date(),"小区间均价.xlsx"),row.names = FALSE)


dk_room_price_data <- left_join (dk_room_data, xiaoqu_std_price %>% ungroup() %>% select(city, community, xiaoqu_std_price_final) %>% distinct(), 
                                 by =c("city","community"), copy = FALSE, suffix = c(".dk_room_data", ".xiaoqu_std_price")) %>% 
  distinct() %>% 
  left_join (xiaoqu_std_price %>% select(city, block.dk_data, block_std_price) %>% distinct(),  
             by =c("city" = "city","block" = "block.dk_data"), copy = FALSE, suffix = c(".dk_room_data", ".xiaoqu_std_price")) %>% 
  distinct() 

dk_room_price_data %>% 
  openxlsx::write.xlsx(paste("Results/",Sys.Date(),"定价中间数据.xlsx"),row.names=F)

# dk_room_price_data %>% filter(is.na(xiaoqu_std_price_final)) %>%
#   select(city, block, community) %>%
#   distinct() %>%
#   write.csv(paste("Results/", Sys.Date(), "间均价缺失小区.csv"))



####单间价格预估

room_outprice_pre_model_has_balcony_area_model <- xgb.load("Model/room_outprice_pre_model_has_balcony_area.model")

dk_room_price_data <- dk_room_price_data %>% 
  #filter(!is.na(xiaoqu_std_price_final)) %>% 
  mutate( origin_bedroom_num_dev = 2- origin_bedroom_num,
          bedroom_num_dev = 3 - bedroom_num,
          fix_area = living_area + balcony_area,
          fix_area_dev = 12 - fix_area,
          face_dev = 3 - face) 

room_forcast_colnams <- c('origin_bedroom_num', 'bedroom_num', 'toilet_num', 'total_area', 'dw_num', 'dly_num',
                          'has_lift', 'heating', 'floor', 'xianfang_days', 'fix_area', 
                          'code', 'has_toilet', 'has_storeroom', 'has_terrace', 'face_old', 'face', 
                          'lighting', 'is_small_window', 'shape', 'city_level', 'dts',
                          'xiaoqu_std_price_final', 'block_std_price',
                          'origin_bedroom_num_dev', 'bedroom_num_dev', 'fix_area_dev', 'face_dev')

dk_room_price_data1 <- select(dk_room_price_data, room_forcast_colnams) %>% 
  sapply(as.numeric)

dk_room_price_data <- mutate(dk_room_price_data, 
                             room_outprice_pre = xiaoqu_std_price_final - predict(room_outprice_pre_model_has_balcony_area_model, dk_room_price_data1)) %>% 
  mutate(adj_room_outprice_pre = ifelse(room_outprice_pre %% 100 <=30, room_outprice_pre - room_outprice_pre %% 100 + 30, room_outprice_pre),
         adj_room_outprice_pre = ifelse(room_outprice_pre %% 100 <=60 & room_outprice_pre %% 100 >30, room_outprice_pre - room_outprice_pre %% 100 + 60, adj_room_outprice_pre),
         adj_room_outprice_pre = ifelse(room_outprice_pre %% 100 >60, room_outprice_pre - room_outprice_pre %% 100 + 90, adj_room_outprice_pre))


############出房价兜底策略(北京折算完免租期26个月内回本，其他城市折算完免租期28个月内回本)

dk_room_price_data_adj <- dk_room_price_data %>%
  group_by(suite_id) %>% 
  mutate(totla_price = sum(adj_room_outprice_pre),
         total_price_fix = totla_price * 1.08,
         gross_margin = 1 -  month_price / (total_price_fix),
         period_of_cost_recovery = decoration_cost / (total_price_fix - month_price),
         gross_margin_mianzuqi = 1 - (1 - (free_days - 25 - 5 * imonths / 12) / 30.4 / imonths) * month_price / (total_price_fix),
         period_of_cost_recovery_mianzuqi = decoration_cost / (total_price_fix - (1 - (free_days - 25 - 5 * imonths / 12) / 30.4 / imonths) * month_price),
         adj_total_price_fix = ifelse(city == '北京市', decoration_cost /26 + (1 - (free_days - 25 - 5 * imonths / 12) / 30.4 / imonths) * month_price,
                                  decoration_cost /28 + (1 - (free_days - 25 - 5 * imonths / 12) / 30.4 / imonths) * month_price),
         adj_total_price = adj_total_price_fix / 1.08,
         pct = adj_room_outprice_pre / totla_price,
         adj_room_outprice_pre_new = adj_total_price * pct) %>% 
  mutate(adj_room_outprice_new = ifelse(adj_room_outprice_pre_new %% 100 <=30, adj_room_outprice_pre_new - adj_room_outprice_pre_new %% 100 + 30, adj_room_outprice_pre_new),
         adj_room_outprice_new = ifelse(adj_room_outprice_pre_new %% 100 <=60 & adj_room_outprice_pre_new %% 100 >30, adj_room_outprice_pre_new - adj_room_outprice_pre_new %% 100 + 60, adj_room_outprice_new),
         adj_room_outprice_new = ifelse(adj_room_outprice_pre_new %% 100 >60, adj_room_outprice_pre_new - adj_room_outprice_pre_new %% 100 + 90, adj_room_outprice_new)) %>% 
  
  mutate(room_price_final = case_when (round(period_of_cost_recovery_mianzuqi,0) <= 0 ~ adj_room_outprice_new,
                                       city == '北京市' & round(period_of_cost_recovery_mianzuqi,0) <= 26 ~ adj_room_outprice_pre,
                                       city == '北京市' & round(period_of_cost_recovery_mianzuqi,0) > 26 ~ adj_room_outprice_new,
                                       city != '北京市' & round(period_of_cost_recovery_mianzuqi,0) > 28 ~ adj_room_outprice_new,
                                       city != '北京市' & round(period_of_cost_recovery_mianzuqi,0) <= 28 ~ adj_room_outprice_pre)) %>% 
  mutate(totla_price_final = sum(room_price_final),
         total_price_final_fix = totla_price_final * 1.08,
         gross_margin_final = 1 -  month_price / (total_price_final_fix),
         period_of_cost_recovery_final = decoration_cost / (total_price_final_fix - month_price),
         gross_margin_mianzuqi_final = 1 - (1 - (free_days - 25 - 5 * imonths / 12) / 30.4 / imonths) * month_price / (total_price_final_fix),
         period_of_cost_recovery_mianzuqi_final = decoration_cost / (total_price_final_fix - (1 - (free_days - 25 - 5 * imonths / 12) / 30.4 / imonths) * month_price)
         )
                                   
         

openxlsx::write.xlsx(dk_room_price_data_adj, paste("Results/",Sys.Date(),"新房定价结果new.xlsx"),row.names=F)


