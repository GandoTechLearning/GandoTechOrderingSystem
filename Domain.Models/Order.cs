using System;

namespace Domain.Models
{
    public class Order
    {
        public Guid Id { get; set; }
        public int ProductId { get; set; }
        public string ProductName { get; set; }
        public int Quantity { get; set; }
        public int Price { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}
