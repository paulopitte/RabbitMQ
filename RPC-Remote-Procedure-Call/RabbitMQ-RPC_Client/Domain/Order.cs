﻿
namespace Domain
{
    public class Order
    {
        public Order(decimal amount)
        {
            Id = DateTime.Now.Ticks;
            Amount = amount;
            OrderStatus = OrderStatus.Processing;
        }

        public long Id { get; set; }
        public decimal Amount { get; set; }
        public string Status => OrderStatus.ToString();
        private OrderStatus OrderStatus { get; set; }


    }

    public enum OrderStatus
    {

        Processing = 0,
        Aproved = 1,
        Declined = 2


    }
}
