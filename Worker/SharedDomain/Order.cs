namespace RabbitMqWorker.Domain;
public sealed record Order
{
    public long Id { get; set; }
    public string ItemName { get; set; }
    public decimal Price { get; set; } = 0M;
}

