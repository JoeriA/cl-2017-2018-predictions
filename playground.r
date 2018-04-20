library("RSQLite")
library('dplyr')
library('tibble')
library('readr')
library('xgboost')
library('lubridate')
library('ranger')

convert_column_to_dummies <- function(df, colname, remove_one = TRUE) {
  # extract column
  column <- as.character(df[[colname]])
  column[is.na(column)] <- ''
  if(remove_one) {
    # create dummies dataframe
    dummies <- data.frame(model.matrix(~column))
    # remove intercept column (will be added in models)
    dummies$X.Intercept. <- NULL
  } else {
    # create dummies dataframe
    dummies <- data.frame(model.matrix(~column +0))
    dummies <- dummies[,colnames(dummies) != 'column']
  }
  # set colnames
  colnames(dummies) <- gsub('column', paste0(colname, "_"), colnames(dummies))
  
  # add dummies to dataframe
  df <- bind_cols(df, dummies)
  # remove original column
  df[[colname]] <- NULL
  return(df)
}

xgb_model <- function(train_data, test_data, independent_vars, dependent_var) {
  train_data_XGB <- xgb.DMatrix(data = as.matrix(train_data[, independent_vars]), label = train_data[[dependent_var]])
  test_data_XGB <- xgb.DMatrix(data = as.matrix(test_data[, independent_vars]), label = test_data[[dependent_var]])
  
  model_XGB <- xgb.train(data = train_data_XGB, booster = "gblinear",
                         params = list(objective = 'multi:softmax', num_class = length(unique((train_data[[dependent_var]])))),
                         nrounds = 100)
  
  fit_XGB <- predict(model_XGB, train_data_XGB)
  fc_XGB <- predict(model_XGB, test_data_XGB)
  
  train_data[[paste(dependent_var, 'prediction', sep = '_')]] <- fit_XGB
  test_data[[paste(dependent_var, 'prediction', sep = '_')]] <- fc_XGB
  return(list(train_data = train_data, test_data = test_data))
}

rf_model <- function(train_data, test_data, independent_vars, dependent_var) {
  train_data[[dependent_var]] <- as.factor(train_data[[dependent_var]])
  test_data[[dependent_var]] <- as.factor(test_data[[dependent_var]])
  # Run model
  formula <- paste0(dependent_var, "~ `",
                    paste(independent_vars, collapse="` + `"), "`")
  model_RF <- ranger(formula=as.formula(formula), data=train_data)
  
  fc_RF <- predictions(predict(model_RF, data = test_data))
  fit_RF <- predictions(predict(model_RF, data = train_data))
  
  train_data[[paste(dependent_var, 'prediction', sep = '_')]] <- fit_RF
  test_data[[paste(dependent_var, 'prediction', sep = '_')]] <- fc_RF
  return(list(train_data = train_data, test_data = test_data))
}

## list all tables
con_sql <- DBI::dbConnect(RSQLite::SQLite(), dbname="database.sqlite")
tables <- dbListTables(con_sql)
dbDisconnect(con_sql)

## connect to db
con <- src_sqlite("database.sqlite")


country_head <- tbl(con, 'Country') %>% dplyr::select(-id) %>% as_tibble()
league_head <- tbl(con, 'League') %>% dplyr::select(-id) %>% as_tibble()
match_head <- tbl(con, 'Match') %>% dplyr::select(-id) %>% as_tibble()
player_info_head <- tbl(con, 'Player') %>% dplyr::select(-id) %>% as_tibble()
player_attributes_head <- tbl(con, 'Player_Attributes') %>% dplyr::select(-id) %>% as_tibble()
team_info_head <- tbl(con, 'Team') %>% dplyr::select(-id) %>% as_tibble()
team_attributes_head <- tbl(con, 'Team_Attributes') %>% dplyr::select(-id) %>% as_tibble()
sqlite_sequence_head <- tbl(con, 'sqlite_sequence') %>% as_tibble()


country <- tbl(con, 'Country') %>% dplyr::select(-id) # %>% as_tibble()
league <- tbl(con, 'League') %>% dplyr::select(-id)# %>% as_tibble()
match <- tbl(con, 'Match') %>% dplyr::select(-id)# %>% as_tibble()
player_info <- tbl(con, 'Player') %>% dplyr::select(-id)# %>% as_tibble()
player_attributes <- tbl(con, 'Player_Attributes') %>% dplyr::select(-id)# %>% as_tibble()
team_info <- tbl(con, 'Team') %>% dplyr::select(-id)# %>% as_tibble()
team_attributes <- tbl(con, 'Team_Attributes') %>% dplyr::select(-id)# %>% as_tibble()

# remove unused columns
player_rating <- player_attributes %>% dplyr::select(date_player = date, player_api_id, overall_rating) %>% as_tibble()

match_data <- match %>% dplyr::select(match_api_id, date, home_team_api_id, away_team_api_id, home_team_goal, away_team_goal,
                                      matches('_player_\\d')) %>%
  as_tibble()

# merge player ratings
for (team in c('home', 'away')) {
  for (i in 1:11) {
    match_data <- match_data %>% dplyr::rename_(.dots=setNames(paste0(team, '_player_', i), 'player_api_id')) %>% 
      left_join(
        player_rating,
        by = c('player_api_id')
      ) %>% dplyr::rename_(.dots=setNames('overall_rating', paste0(team, '_rating_player_', i))) %>%
      filter(date > date_player) %>% group_by(match_api_id) %>% arrange(match_api_id, date_player) %>%
      summarise_all(.funs = funs(last)) %>% dplyr::select(-date_player, -player_api_id)
  }
}

# aggregate player scores of teams
match_data$home_avg_rating <- match_data %>% dplyr::select(matches('home_rating_player')) %>% rowMeans(na.rm = TRUE)
match_data <- match_data %>% dplyr::select(-matches('home_rating_player'))
match_data$away_avg_rating <- match_data %>% dplyr::select(matches('away_rating_player')) %>% rowMeans(na.rm = TRUE)
match_data <- match_data %>% dplyr::select(-matches('away_rating_player'))

# add team data
team_data <- inner_join(team_info,
                        team_attributes,
                        by = c('team_api_id', 'team_fifa_api_id')) %>%
  dplyr::select(team_api_id, team_long_name, date,
                buildUpPlaySpeed, buildUpPlayDribblingClass, buildUpPlayPassing, buildUpPlayPositioningClass,
                chanceCreationPassing, chanceCreationCrossing, chanceCreationShooting, chanceCreationPositioningClass,
                defencePressure, defenceAggression, defenceTeamWidth, defenceDefenderLineClass) %>%
  as_tibble()

team_data_home <- team_data
colnames(team_data_home) <- paste('home', colnames(team_data_home), sep='_')
match_data <- left_join(
  match_data,
  team_data_home,
  by = c('home_team_api_id')
) %>% filter(date > home_date) %>%
  group_by(match_api_id) %>% arrange(match_api_id, home_date) %>%
  summarise_all(.funs = funs(last)) %>% dplyr::select(-home_date, -home_team_api_id)
rm(team_data_home)

team_data_away <- team_data
colnames(team_data_away) <- paste('away', colnames(team_data_away), sep='_')
match_data <- left_join(
  match_data,
  team_data_away,
  by = c('away_team_api_id')
) %>% filter(date > away_date) %>%
  group_by(match_api_id) %>% arrange(match_api_id, away_date) %>%
  summarise_all(.funs = funs(first)) %>% dplyr::select(-away_date, -away_team_api_id)
rm(team_data_away)

match_data <- match_data %>% dplyr::select(-match_api_id) %>%
  mutate(result = ifelse(home_team_goal > away_team_goal, 0,
                         ifelse(home_team_goal < away_team_goal, 2,
                                1))) %>% mutate(date = as_date(date))

# add new matches
new_matches <- read_csv('new_matches2.csv') %>% mutate(date = dmy(date))
newest_team_stats <- read_csv('newest_team_stats.csv')
newest_team_stats_away <- newest_team_stats
colnames(newest_team_stats_away) <- gsub('home', 'away', colnames(newest_team_stats_away))
new_match_data <- new_matches %>% left_join(
  newest_team_stats, by = c('home_team_long_name' = 'team_name')
) %>% left_join(
  newest_team_stats_away, by = c('away_team_long_name' = 'team_name')
)

new_match_data <- bind_rows(match_data, new_match_data)

# write_csv(match_data, 'data_extended.csv')

dbDisconnect(con$con)

# create more variables
new_match_data <- new_match_data %>%
  convert_column_to_dummies('home_buildUpPlayDribblingClass') %>%
  convert_column_to_dummies('home_buildUpPlayPositioningClass') %>%
  convert_column_to_dummies('home_chanceCreationPositioningClass') %>%
  convert_column_to_dummies('home_defenceDefenderLineClass') %>%
  convert_column_to_dummies('away_buildUpPlayDribblingClass') %>%
  convert_column_to_dummies('away_buildUpPlayPositioningClass') %>%
  convert_column_to_dummies('away_chanceCreationPositioningClass') %>%
  convert_column_to_dummies('away_defenceDefenderLineClass')
new_match_data <- new_match_data %>% mutate(diff_rating = home_avg_rating - away_avg_rating)

# model part
train_data <- new_match_data %>% filter(date < as.Date('2015-01-01'))
test_data <- new_match_data %>% filter(date >= as.Date('2015-01-01')) %>% filter(date < as.Date('2017-01-01'))
independent_vars <- colnames(new_match_data %>% dplyr::select(-date, -matches('goal'), -matches('name'), -matches('result')))
# independent_vars <- c('avg_rating_home', 'avg_rating_away')

data_fc <- xgb_model(train_data = train_data, test_data = test_data,
                     independent_vars = independent_vars, dependent_var = 'home_team_goal')
train_data <- data_fc$train_data
test_data <- data_fc$test_data

train_data$home_team_goal_prediction <- round(as.numeric(train_data$home_team_goal_prediction))
test_data$home_team_goal_prediction <- round(as.numeric(test_data$home_team_goal_prediction))

data_fc <- xgb_model(train_data = train_data, test_data = test_data,
                     independent_vars = independent_vars, dependent_var = 'away_team_goal')
train_data <- data_fc$train_data
test_data <- data_fc$test_data

train_data$away_team_goal_prediction <- round(as.numeric(train_data$away_team_goal_prediction))
test_data$away_team_goal_prediction <- round(as.numeric(test_data$away_team_goal_prediction))

data_fc <- xgb_model(train_data = train_data, test_data = test_data,
                     independent_vars = independent_vars, dependent_var = 'result')
train_data <- data_fc$train_data
test_data <- data_fc$test_data

train_data <- train_data %>%
  mutate(result_prediction2 = ifelse(home_team_goal_prediction > away_team_goal_prediction, 0,
                         ifelse(home_team_goal_prediction < away_team_goal_prediction, 2,
                                1)))
test_data <- test_data %>%
  mutate(result_prediction2 = ifelse(home_team_goal_prediction > away_team_goal_prediction, 0,
                                    ifelse(home_team_goal_prediction < away_team_goal_prediction, 2,
                                           1)))

cat('correct result train', mean(train_data$result == train_data$result_prediction))
cat('correct result2 train', mean(train_data$result == train_data$result_prediction2))
cat('correct home goals train', mean(train_data$home_team_goal == train_data$home_team_goal_prediction))
cat('correct away goals train', mean(train_data$away_team_goal == train_data$away_team_goal_prediction))
# table(train_data$result, train_data$result_prediction)
cat('correct result test', mean(test_data$result == test_data$result_prediction))
cat('correct result2 test', mean(test_data$result == test_data$result_prediction2))
cat('correct home goals test', mean(test_data$home_team_goal == test_data$home_team_goal_prediction))
cat('correct away goals test', mean(test_data$away_team_goal == test_data$away_team_goal_prediction))
table(test_data$result, test_data$result_prediction)

# predict new matches
train_data <- new_match_data %>% filter(date < as.Date('2017-01-01'))
test_data <- new_match_data %>% filter(date >= as.Date('2017-01-01'))

data_fc <- xgb_model(train_data = train_data, test_data = test_data,
                     independent_vars = independent_vars, dependent_var = 'home_team_goal')
train_data <- data_fc$train_data
test_data <- data_fc$test_data

train_data$home_team_goal_prediction <- round(as.numeric(train_data$home_team_goal_prediction), 2)
test_data$home_team_goal_prediction <- round(as.numeric(test_data$home_team_goal_prediction), 2)

data_fc <- xgb_model(train_data = train_data, test_data = test_data,
                     independent_vars = independent_vars, dependent_var = 'away_team_goal')
train_data <- data_fc$train_data
test_data <- data_fc$test_data

train_data$away_team_goal_prediction <- round(as.numeric(train_data$away_team_goal_prediction), 2)
test_data$away_team_goal_prediction <- round(as.numeric(test_data$away_team_goal_prediction), 2)

data_fc <- xgb_model(train_data = train_data, test_data = test_data,
                     independent_vars = independent_vars, dependent_var = 'result')
train_data <- data_fc$train_data
test_data <- data_fc$test_data

train_data <- train_data %>%
  mutate(result_prediction2 = ifelse(home_team_goal_prediction > away_team_goal_prediction, 0,
                                    ifelse(home_team_goal_prediction < away_team_goal_prediction, 2,
                                           1)))
test_data <- test_data %>%
  mutate(result_prediction2 = ifelse(home_team_goal_prediction > away_team_goal_prediction, 0,
                                    ifelse(home_team_goal_prediction < away_team_goal_prediction, 2,
                                           1)))

predictions <- test_data %>% dplyr::select(date, home_team_long_name, home_team_goal_prediction,
                                           away_team_goal_prediction, away_team_long_name,
                                           result_prediction, result_prediction2)

predictions <- predictions %>% mutate(home_points = ifelse(result_prediction2 == 0, 3,
                                                           ifelse(result_prediction2 == 1, 1,
                                                                  0)),
                                          away_points = ifelse(result_prediction2 == 0, 0,
                                                           ifelse(result_prediction2 == 1, 1,
                                                                  3)))

points <- bind_rows(
  predictions %>% select(team = home_team_long_name, points = home_points),
  predictions %>% select(team = away_team_long_name, points = away_points)
) %>% group_by(team) %>% summarise(points = sum(points)) %>% arrange(-points)
