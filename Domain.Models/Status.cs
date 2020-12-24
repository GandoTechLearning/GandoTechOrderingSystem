using System;
using System.Collections.Generic;
using System.Text;

namespace Domain.Models
{
    public enum Status
    {
        OrderSubmitted,
        OrderValidated,
        OrderOutOfStock,
        PaymentProcessed,
        PaymentFailed,
        OrderDispatched
    }
}
