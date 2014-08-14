CREATE PROCEDURE [work].[CancelNodeInvocations]
	@InstanceName nvarchar(100)
AS
	INSERT INTO [private].InvocationsStore(
			[Id],
            [Job],
            [Source],
            [Payload],
            [Status],
            [Result],
            [ResultMessage],
            [UpdatedBy],
            [LogUrl],
            [DequeueCount],
            [IsContinuation],
            [Complete],
            [LastDequeuedAt],
            [LastSuspendedAt],
            [CompletedAt],
            [QueuedAt],
            [NextVisibleAt],
            [UpdatedAt],
            [JobInstanceName])
	OUTPUT	inserted.*
	SELECT	Id,
			Job, 
			Source, 
			Payload, 
			5 AS [Status] /* Cancelled */,
			4 AS [Result] /* Aborted */,
            'Node was reinitialized' AS [ResultMessage],
			@InstanceName AS [UpdatedBy],
            [LogUrl],
			DequeueCount,
			IsContinuation,
			Complete,
            [LastDequeuedAt],
            [LastSuspendedAt],
            [CompletedAt],
			QueuedAt,
			[NextVisibleAt],
			SYSUTCDATETIME() AS [UpdatedAt],
            [JobInstanceName]
	FROM	[work].ActiveInvocations
	WHERE	[UpdatedBy] = @InstanceName
	AND		[Status] = 2 /* Dequeued */ OR [Status] = 3 /* Executing */