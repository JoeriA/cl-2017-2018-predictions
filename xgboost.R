#'  @title: xgboost for vf_framework
#'  @Author: Joeri Admiraal
#'  @description
#'  Company: Veneficus
#'  Client: General
#'  Input: 
#'  

library(xgboost)

# Define functions
xgb <- function(dataSample, dataForecast, dependent_vars, external_vars, predBound, ...) {
  result <- map(dependent_vars, function(x) {
    xgb_one_var(dataSample=dataSample, dataForecast=dataForecast, dependent_var=x,
                independent_vars=external_vars, predBound=predBound)
  })
  fit_fore_XGB <- suppressWarnings(map(result, function(x) {x$fit_fore}) %>% bind_cols())
  coef_XGB <- suppressWarnings(map(result, function(x) {x$coef}) %>% bind_rows())
  
  return(list(fit_fore_XGB=fit_fore_XGB, coef_XGB=coef_XGB))
}

xgb_one_var <- function(dataSample, dataForecast, dependent_var,
                        independent_vars, predBound) {
  
  # Remove variables that do not exist in the dataset
  independent_vars <- map_chr(independent_vars, function(x) ifelse(is.null(dataSample[[x]]), NA, x)) %>%
    na.omit() %>% as.character()
  
  # Run model
  dataSample_XGB <- xgb.DMatrix(data = as.matrix(dataSample[, independent_vars]), label = dataSample[[dependent_var]])
  dataForecast_XGB <- xgb.DMatrix(data = as.matrix(dataForecast[, independent_vars]), label = dataForecast[[dependent_var]])
  
  model_XGB <- xgb.train(data = dataSample_XGB, booster = "gblinear", nrounds = 100)
  
  fc_XGB <- predict(model_XGB, dataForecast_XGB)
  fit_XGB <- predict(model_XGB, dataSample_XGB)
  
  coef_XGB <- tibble(name=independent_vars, included=TRUE, Model='xgboost', Fact=dependent_var)
  
  # Save fits and forecasts
  fit_fore_XGB <- data.frame(
    dependent_var=c(fit_XGB, fc_XGB), stringsAsFactors=FALSE)
  colnames(fit_fore_XGB)[1] <- paste('xgboost', dependent_var, sep="_")
  
  return(list(fit_fore_XGB=fit_fore_XGB, coef_XGB=coef_XGB))
}

