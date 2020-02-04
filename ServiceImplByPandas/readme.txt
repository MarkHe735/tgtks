01          RowPartitioningService              按照比例拆分数据            表操作
02          QualityProfilingService             对数据质量进行画像          表操作
03          ColumnCleaningService               清洗低质量数据列            列操作
04          RowCleaningService                  清洗低质量数据行            行操作
05          ColumnPartitioningByTypeService     按照列类型拆分数据          列操作
06          NumberColumnDataProfilingService    对数值列进行画像            表操作
07          CategoryColumnDataProfilingService  对类别列进行画像            表操作
08          LowCardColumnFilteringService       过滤低基数数值列            列操作
09          ToCategoryConvertingService         数值列转类别列         　   表操作
10          CategoryColumnMissingValueImputingService 类别列缺失值填充      表操作
11          NumberColumnMissingValueImputingService 数值列缺失值填充        表操作
12          NumberColumnOutlierProcessingService    数值列异常值处理        表操作
13          NumberColumnBinningService          数值列自动分箱
14          NumberColumnBinningApplyService     数值列自动分箱应用
15          CategoryColumnEncodingService       类别列编码
16          CategoryColumnEncodingApplyService  类别列编码应用
17          ColumnFilteringService              列过滤
18          ColumnConcatingService              列拼接
19          XGboostGridSearchingService         XGBoost网格搜索建模
20          XGboostPredictingService            XGBoost模型预测
21          CSVDataReadingService               读取CSV文件
22          DateTimeColumnFeatureGeneratingService 时间日期类别特征生成
23          SupervisedCategoryColumnEncodingService 有监督类别编码
24          SupervisedCategoryColumnEncodingApplyService 有监督类别编码应用
25          XGboostHyperOptimizingService       XGBoost贝叶斯优化建模
26          CatBoostHyperOptimizingService      CatBoost优化建模
27          LightGBMHyperOptimizingService      LightGBM优化建模
28          KFlodSupervisedCategoryColumnEncodingService 有监督类别列编码服务
29          KFlodSupervisedCategoryColumnEncodingApplyService 有监督类别列编码应用服务
30          KFlodSupervisedCategoryColumnEncodingApplyService2 有监督类别列编码应用服务
31          EntityFeaturesService               featuretools特征衍生(不含时间窗功能)