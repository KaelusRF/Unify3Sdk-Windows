#:package Kaelus.Unify3Sdk@0.1.2
using Kaelus.Unify3Sdk;
using System.Globalization;

var unify = Unify.Instance;
string? connectedInstrumentSerialNumber = null;

try
{
    // Ask the SDK to emit the log levels that are useful for this example.
    unify.SetLogLevels(LogLevel.Info, LogLevel.Error);

    using var logCancellation = new CancellationTokenSource();

    // Read the SDK log stream on a background task so logging can continue
    // while the rest of the workflow discovers and tests an instrument.
    var logTask = Task.Run(async () =>
    {
        try
        {
            await foreach (var log in unify.GetLogs(logCancellation.Token))
            {
                var timestamp = log.Time.ToString("O", CultureInfo.InvariantCulture);
                Console.WriteLine($"[{timestamp}|{log.Level}|{log.Logger}] {log.Message}");
            }
        }
        catch (OperationCanceledException) when (logCancellation.IsCancellationRequested)
        {
        }
    });

    try
    {
        Instrument? instrument = null;

        // Give discovery an explicit timeout so the example fails clearly
        // instead of waiting forever when no iWA is nearby.
        using var discoveryTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        try
        {
            // RunBluetoothScan stays active while the SDK is scanning, so we
            // keep the returned task alive in the background while discovery
            // waits for instruments to appear.
            _ = unify.RunBluetoothScan();

            // Discovery yields the instruments the SDK can currently see.
            // Here we pick the first iWA so the rest of the example has
            // something concrete to connect to.
            await foreach (var instruments in unify.RunInstrumentDiscovery(discoveryTimeout.Token))
            {
                instrument = instruments.FirstOrDefault(candidate => candidate.Type == InstrumentType.Iwa);

                if (instrument is not null)
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) when (discoveryTimeout.IsCancellationRequested)
        {
            throw new System.TimeoutException("Timed out waiting for an iWA instrument to be discovered.");
        }
        finally
        {
            // Stopping the scan is what tells the SDK to end Bluetooth
            // discovery. Await the task so it can shut down
            // cleanly before the example continues.
            await unify.StopBluetoothScan();
        }

        if (instrument is null)
        {
            throw new InvalidOperationException("Instrument discovery completed without finding an iWA instrument.");
        }

        // Connect to the instrument we just discovered.
        Console.WriteLine($"Connecting to iWA {instrument.SerialNumber}...");
        var connectionResult = await unify.ConnectInstrument(instrument.SerialNumber);

        if (connectionResult != ConnectionResult.Success)
        {
            throw new InvalidOperationException($"Connecting to {instrument.SerialNumber} failed with result {connectionResult}.");
        }

        connectedInstrumentSerialNumber = instrument.SerialNumber;

        // Read the connected instrument definition so the test uses settings
        // that are valid for the specific hardware model.
        if (unify.GetInstrumentDefinition(instrument.SerialNumber) is not CaaInstrumentDefinition instrumentDefinition)
        {
            throw new InvalidOperationException("The connected instrument did not provide a CAA instrument definition.");
        }

        // Define a single return loss vs frequency trace across the
        // instrument's supported frequency range.
        var test = new Test
        {
            Traces = new List<Trace>
            {
                new CaaReturnLossFrequencyTrace
                {
                    // Complex format returns each point as I/Q data instead
                    // of a single scalar magnitude.
                    Format = CaaReturnLossFrequencyTraceFormat.Complex,
                    InstrumentSerialNumber = instrument.SerialNumber,
                    InstrumentModel = instrumentDefinition.Model,
                    FrequencyRange_Hz = instrumentDefinition.FrequencyRange_Hz,
                    NumberOfPoints = 401,
                    Duration_s = 60,
                }
            },
            Cardinality = Cardinality.Single,
        };

        // Configure the test before running it so the SDK can validate the
        // requested trace against the connected instrument.
        var testConfigurationResult = unify.ConfigureTest(test);

        if (!testConfigurationResult.Valid)
        {
            throw new InvalidOperationException("The SDK rejected the return loss test configuration.");
        }

        Console.WriteLine("Starting test");

        // RunTest streams progress updates while the instrument executes the configured measurement.
        await foreach (var progressUpdate in unify.RunTest(checkRl: true, CancellationToken.None))
        {
            if (progressUpdate.TraceProgress < 0)
            {
                Console.WriteLine("Progress: measuring...");
            }
            else
            {
                var percentage = (int)Math.Round(progressUpdate.TraceProgress * 100);
                Console.WriteLine($"Progress: {percentage}%");
            }
        }

        Console.WriteLine("Test complete!");

        // After the test finishes, fetch the first batched result and inspect the returned trace data.
        BatchedTestResult? testResult = await unify.GetBatchedTestResults().FirstOrDefaultAsync()
            ?? throw new InvalidOperationException("The test finished, but the SDK did not return any batched results.");

        Console.WriteLine($"Got {testResult.TracePoints.Count} points.");
    }
    finally
    {
        if (connectedInstrumentSerialNumber is not null)
        {
            await unify.DisconnectInstrument(connectedInstrumentSerialNumber);
        }

        logCancellation.Cancel();
        await logTask;
    }
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Example failed: {ex.Message}");
    Environment.ExitCode = 1;
}
