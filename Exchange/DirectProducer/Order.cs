using System;

namespace DirectProducer
{
    internal class Order
    {
        private const short UTC = -3;
        public Order(long id, long amount)
        {
            Id = id;
            Amount = amount;
            CreateDate = DateTime.UtcNow.AddHours(UTC);
        }

        public long Id { get; private set; }
        public DateTime CreateDate { get; }
        public DateTime LastUpdate { get; private set; }
        public long Amount { get; set; }


        public void UpdateOrder(long amount)
        {
            Amount = amount;
            LastUpdate = DateTime.UtcNow.AddHours(UTC);
        }

    }
}
