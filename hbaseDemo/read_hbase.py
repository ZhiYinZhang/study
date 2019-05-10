cols={
                  "value":"order_sum",
                  "abnormal":"sum_abno_month",
                  "mean_plus_3std":"grade_sum_plus3",
                  "mean_minus_3std":"grade_sum_minu3",
                  "mean":"grade_sum"
                  }
values=list(cols.values())
values.remove(cols["value"])

print(values)