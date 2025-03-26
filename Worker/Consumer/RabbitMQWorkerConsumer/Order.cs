﻿namespace Domain;
public sealed record Order
{
    public int Id { get; set; }
    public string ItemName { get; set; }
    public decimal Price { get; set; }
}

