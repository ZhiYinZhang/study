def get_ratio_monthly():
    # 本县或区所有零售户上 上个月，上3个月，上6个月，上12个月 所订省内烟、省外烟、进口烟的占比（省内外烟）
    # 本县或区所有零售户上 上个月，上3个月，上6个月，上12个月 订购数前5省内烟/省外
    # 本县或区所有零售户上 上个月，上3个月，上6个月，上12个月 所订卷烟价类占比
    # 本县或区所有零售户上 上个月，上3个月，上6个月，上12个月 所订卷烟价格分段占比
    try:
        colNames = ["item_id", "qty_ord", "price", "sale_center_id", "month_diff"]
        #获取过去12个的订单行表数据
        co_co_line = get_co_co_line(spark, scope=[1, 12], filter="month").select(colNames)

        #获取烟基本信息
        plm_item = get_plm_item(spark).select("item_id", "yieldly_type", "kind", "item_name")

        line_plm = co_co_line.join(plm_item, "item_id")

        # 总量
        # 上个月
        total_month_1 = co_co_line.where(col("month_diff") <= 1) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord"))
        # 上3个月
        total_month_3 = co_co_line.where(col("month_diff") <= 3) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord"))
        # 上6个月
        total_month_6 = co_co_line.where(col("month_diff") <= 6) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord"))
        # 上12个月
        total_month_12 = co_co_line.where(col("month_diff") <= 12) \
            .groupBy("sale_center_id") \
            .agg(f.sum("qty_ord").alias("qty_ord"))


        days = {"1_month": 1, "3_month": 3, "6_month": 6, "12_month": 12}
        days_total = {"1_month": total_month_1, "3_month": total_month_3,
                      "6_month": total_month_6, "12_month": total_month_12}

        # 省内 0  省外 1 国外 3
        yieldly_types = ["0", "1", "3"]
        cols1 = {
            "1_month": ["in_prov_last_month", "out_prov_last_month", "import_last_month"],
            "3_month": ["in_prov_last_three_month", "out_prov_last_three_month", "import_last_three_month"],
            "6_month": ["in_prov_last_half_year", "out_prov_last_half_year", "import_last_half_year"],
            "12_month": ["in_prov_last_year", "out_prov_last_year", "import_last_year"]
        }
        # 省内 0 省外 1 的top5
        cols2 = {
            "1_month": ["in_prov_top5_last_month", "out_prov_top5_last_month"],
            "3_month": ["in_prov_top5_last_three_month", "out_prov_top5_last_three_month"],
            "6_month": ["in_prov_top5_last_half_year", "out_prov_top5_last_half_year"],
            "12_month": ["in_prov_top5_last_year", "out_prov_top5_last_year"]
        }
        # 1:一类,2:二类,3:三类,4:四类,5:五类,6:无价类
        kinds = ["1", "2", "3", "4", "5", "6"]
        cols3 = {
            "1_month": ["price_ratio_last_month_1", "price_ratio_last_month_2", "price_ratio_last_month_3",
                        "price_ratio_last_month_4", "price_ratio_last_month_5", "price_ratio_last_month_6"],
            "3_month": ["price_ratio_last_three_month_1", "price_ratio_last_three_month_2",
                        "price_ratio_last_three_month_3", "price_ratio_last_three_month_4",
                        "price_ratio_last_three_month_5", "price_ratio_last_three_month_6"],
            "6_month": ["price_ratio_last_half_year_1", "price_ratio_last_half_year_2", "price_ratio_last_half_year_3",
                        "price_ratio_last_half_year_4", "price_ratio_last_half_year_5", "price_ratio_last_half_year_6"],
            "12_month": ["price_ratio_last_year_1", "price_ratio_last_year_2", "price_ratio_last_year_3",
                         "price_ratio_last_year_4", "price_ratio_last_year_5", "price_ratio_last_year_6"]
        }

        # 50以下；50（含）-100，100（含）-300，300（含）-500，500（含）以上
        prices = [0, 50, 100, 300, 500]
        cols4 = {
            "1_month": ["price_sub_last_month_under50", "price_sub_last_month_50_to_100",
                        "price_sub_last_month_100_to_300", "price_sub_last_month_300_to_500",
                        "price_sub_last_month_up500"],
            "3_month": ["price_sub_last_three_month_under50", "price_sub_last_three_month_50_to_100",
                        "price_sub_last_three_month_100_to_300", "price_sub_last_three_month_300_to_500",
                        "price_sub_last_three_month_up500"],
            "6_month": ["price_sub_last_half_year_under50", "price_sub_last_half_year_50_to_100",
                        "price_sub_last_half_year_100_to_300", "price_sub_last_half_year_300_to_500",
                        "price_sub_last_half_year_up500"],
            "12_month": ["price_sub_last_year_under50", "price_sub_last_year_50_to_100",
                         "price_sub_last_year_100_to_300", "price_sub_last_year_300_to_500",
                         "price_sub_last_year_up500"]
        }

        win = Window.partitionBy("sale_center_id").orderBy(f.desc("yieldly_type_qty_ord"))

        for key in days.keys():
            # 对应日期的烟总量
            total_df = days_total[key]
            # 日期过滤条件
            day = days[key]
            #按照日期过滤
            day_filter = line_plm.where((col("month_diff") <= day))


            for i in range(len(yieldly_types)):
                yieldly_type = yieldly_types[i]
                yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)

                # 本县或区所有零售户上个月，上3个月，上6个月，上12个月所订省内烟、省外烟、进口烟的占比（省内外烟）
                colName = cols1[key][i]
                try:
                    print(f"{str(dt.now())} 本县或区上{day}月省内烟/省外烟/进口烟的占比  yieldly_type:{yieldly_type}")
                    yieldly_type_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .join(total_df, "sale_center_id") \
                        .withColumn(colName, col("yieldly_type_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

                # top 5
                # 本县或区所有零售户上个月，上3个月，上6个月，上12个月订购数前5省内烟/省外
                if yieldly_type in ["0", "1"]:
                    print(f"{str(dt.now())}   本县或区上{day}月省内/省外烟前5  yieldly_type:{yieldly_type}")
                    try:
                        colName = cols2[key][i]

                        top5 = yieldly_type_filter.groupBy("sale_center_id", "item_id") \
                            .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 5)
                        top5.join(plm_item, "item_id") \
                            .groupBy("sale_center_id") \
                            .agg(f.collect_list("item_name").alias(colName)) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                    except Exception:
                        tb.print_exc()

            # 本县或区所有零售户上个月，上3个月，上6个月，上12个月所订卷烟价类占比
            for i in range(len(kinds)):
                try:
                    kind = kinds[i]
                    kind_filter = day_filter.where(col("kind") == kind)
                    colName = cols3[key][i]
                    print(f"{str(dt.now())} 本县或区上{day}月卷烟价类占比 kind:{kind}")

                    kind_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("kind_qty_ord")) \
                        .join(total_df, "sale_center_id") \
                        .withColumn(colName, col("kind_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

            # 本县或区所有零售户上个月，上3个月，上6个月，上12个月所订卷烟价格分段占比
            for i in range(len(prices)):
                price = prices[i]
                colName = cols4[key][i]
                if price == 500:
                    price_filter = day_filter.where(col("price") >= price)
                    print(f"{str(dt.now())} 本县或区上{day}月卷烟价格分段占比 price:({price},∞]")
                else:
                    price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
                    print(f"{str(dt.now())}  本县或区上{day}月卷烟价格分段占比 price:({price},{prices[i+1]}]")
                try:
                    price_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("price_qty_ord")) \
                        .join(total_df, "sale_center_id", "left") \
                        .withColumn(colName, col("price_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()
    except Exception:
        tb.print_exc()
# get_ratio()
















def get_ratio_weekly():
    # 本县或区所有零售户上 1周 所订省内烟、省外烟、进口烟的占比（省内外烟）
    # 本县或区所有零售户上 1周 订购数前5省内烟/省外
    # 本县或区所有零售户上 1周 所订卷烟价类占比
    # 本县或区所有零售户上 1周 所订卷烟价格分段占比
    try:
        colNames = ["item_id", "qty_ord", "price", "sale_center_id", "week_diff"]
        #获取上个月订单行表数据
        co_co_line = get_co_co_line(spark, scope=[1,1], filter="week").select(colNames)

        #获取烟基本信息数据
        plm_item = get_plm_item(spark).select("item_id", "yieldly_type", "kind", "item_name")

        line_plm = co_co_line.join(plm_item, "item_id")

        # 本区域上1周所有烟的订单总量
        total_week_1 = co_co_line.groupBy("sale_center_id") \
                                 .agg(f.sum("qty_ord").alias("qty_ord"))

        days = {"1_week": 1}
        days_total = {"1_week": total_week_1}

        # 省内 0  省外 1 国外 3
        yieldly_types = ["0", "1", "3"]
        cols1 = {
            "1_week": ["in_prov_last_week", "out_prov_last_week", "import_last_week"],
        }

        # 省内 0 省外 1 的top5
        cols2 = {
            "1_week": ["in_prov_top5_last_week", "out_prov_top5_last_week"],
        }

        # 1:一类,2:二类,3:三类,4:四类,5:五类,6:无价类
        kinds = ["1", "2", "3", "4", "5", "6"]
        cols3 = {
            "1_week": ["price_ratio_last_week_1", "price_ratio_last_week_2", "price_ratio_last_week_3",
                       "price_ratio_last_week_4", "price_ratio_last_week_5", "price_ratio_last_week_6"],
        }

        # 50以下，50（含）-100，100（含）-300，300（含）-500，500（含）以上
        prices = [0, 50, 100, 300, 500]
        cols4 = {
            "1_week": ["price_sub_last_week_under50", "price_sub_last_week_50_to_100", "price_sub_last_week_100_to_300",
                       "price_sub_last_week_300_to_500", "price_sub_last_week_up500"],
        }

        win = Window.partitionBy("sale_center_id").orderBy(f.desc("yieldly_type_qty_ord"))

        for key in days.keys():
            # 对应日期的烟总量
            total_df = days_total[key]
            # 日期过滤条件
            day = days[key]

            day_filter = line_plm.where(col("week_diff") == day)

            for i in range(len(yieldly_types)):
                yieldly_type = yieldly_types[i]
                yieldly_type_filter = day_filter.where(col("yieldly_type") == yieldly_type)

                # 本县或区所有零售户上 1周 所订省内烟、省外烟、进口烟的占比（省内外烟）
                colName = cols1[key][i]
                try:
                    print(f"{str(dt.now())}  本县或区上周省内烟/省外烟/进口烟的占比 yieldly_type:{yieldly_type}")
                    yieldly_type_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                        .join(total_df, "sale_center_id") \
                        .withColumn(colName, col("yieldly_type_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

                # 本县或区所有零售户上 1周 订购数前5省内烟/省外
                if yieldly_type in ["0", "1"]:
                    print(f"{str(dt.now())}   本县或区上周省内/省外烟前五  yieldly_type:{yieldly_type}")
                    try:
                        colName = cols2[key][i]

                        top5 = yieldly_type_filter.groupBy("sale_center_id", "item_id") \
                            .agg(f.sum("qty_ord").alias("yieldly_type_qty_ord")) \
                            .withColumn("rank", f.row_number().over(win)) \
                            .where(col("rank") <= 5)
                        top5.join(plm_item, "item_id") \
                            .groupBy("sale_center_id") \
                            .agg(f.collect_list("item_name").alias(colName)) \
                            .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                    except Exception:
                        tb.print_exc()

            # 本县或区所有零售户上 1周 所订卷烟价类占比
            for i in range(len(kinds)):
                try:
                    kind = kinds[i]
                    kind_filter = day_filter.where(col("kind") == kind)
                    colName = cols3[key][i]
                    print(f"{str(dt.now())}   本县或区 上一周 所订卷烟价类占比:{kind}")

                    kind_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("kind_qty_ord")) \
                        .join(total_df, "sale_center_id") \
                        .withColumn(colName, col("kind_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()

            # 本县或区所有零售户上 1周 所订卷烟价格分段占比
            for i in range(len(prices)):
                price = prices[i]
                colName = cols4[key][i]
                if price == 500:
                    price_filter = day_filter.where(col("price") >= price)
                    print(f"{str(dt.now())}  本县或区 上一周 所订卷烟价格分段占比:({price},∞]")
                else:
                    price_filter = day_filter.where((col("price") >= price) & (col("price") < prices[i + 1]))
                    print(f"{str(dt.now())}  本县或区 上一周 所订卷烟价格分段占比:({price},{prices[i+1]}]")
                try:
                    price_filter.groupBy("sale_center_id") \
                        .agg(f.sum("qty_ord").alias("price_qty_ord")) \
                        .join(total_df, "sale_center_id", "left") \
                        .withColumn(colName, col("price_qty_ord") / col("qty_ord")) \
                        .foreachPartition(lambda x: write_hbase1(x, [colName], hbase))
                except Exception:
                    tb.print_exc()
    except Exception:
        tb.print_exc()
# get_ratio_weekly()