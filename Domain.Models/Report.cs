using System;
using System.Collections.Generic;
using System.Text;

namespace Domain.Models
{
    public class Report
    {
        public Guid Id { get; set; }
        public Order Order { get; set; }
        public string Details { get; set; }
        public Status Status { get; set; }
        public DateTime CreatedOn { get; set; }
    }
}
