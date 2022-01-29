using Domain;

namespace Services
{
    public  class OrderServices
    {
        public static OrderStatus OnStore(decimal amount)
            => (amount < 0 || amount > 1000) ? OrderStatus.Declined : OrderStatus.Aproved;
    }
}
