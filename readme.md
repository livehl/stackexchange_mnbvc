# stackexchange_mnbvc

# stackexchange 数据清理项目

这是[MNBVC项目](https://github.com/esbatmop/MNBVC)的一部分，负责清理stackexchange的数据为AI框架可以直接使用的数据格式。
stackexchange会定期将自己旗下的站点数据导出到互联网。本项目用于解析导出的数据至json格式。由于其巨大的xml文件python处理不了,因此使用java体系进行处理。


##  特性

1. 支持超大xml解析。

2. 整站解析。

3. 按大小拆分成品文件。

4. 效率高，速度快。

## 使用方法

1. 下载完整数据包 https://archive.org/details/stackexchange 
2. 修改allSitemain方法中的路径
3. 添加 jvm参数 -DentityExpansionLimit=0 -DtotalEntitySizeLimit=0 -Djdk.xml.totalEntitySizeLimit=0 -Djdk.xml.totalEntitySizeLimit=0
4. 执行 allSite

## 注意事项

1. 通过种子下载文件即可。下载回来的文件不需要解压。

2. 生成的文件500M一个，以自然行切分(不会丢失数据)。
