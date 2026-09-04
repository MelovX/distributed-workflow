namespace DistributedWorkflow.Worker.Data
{
    public enum InboxClaimStatus
    {
        Acquired,
        AlreadyCompleted,
        LeaseHeld
    }
}
