using Services;

namespace Domain
{
    public class Order
    {
        public Order(decimal amount)
        {
            Id = DateTime.Now.Ticks;
            Amount = amount;
            OrderStatus = OrderServices.OnStore(amount);
        }

        public long Id { get; set; }
        public decimal Amount { get; set; }
        public string Status => OrderStatus.ToString();
        private OrderStatus OrderStatus { get; set; }

        internal void SetStatus(OrderStatus orderStatus) =>
            OrderStatus = orderStatus;

    }

    public enum OrderStatus
    {
        Processing = 0,
        Aproved = 1,
        Declined = 2
    }
}
