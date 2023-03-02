select * from 
    {{
        metrics.calculate(
            metric('orders_over_time'),
            dimensions= ['status_code']
        )
    }}