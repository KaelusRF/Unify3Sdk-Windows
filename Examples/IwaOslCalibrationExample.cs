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
    // while the rest of the workflow discovers and calibrates an instrument.
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
        // Discovery yields the instruments the SDK can currently see.
        // Here we pick the first iWA so the example has something concrete
        // to connect to and calibrate.
        var instrument = await DiscoverFirstIwa(unify);

        // Connect to the instrument we just discovered.
        Console.WriteLine($"Connecting to iWA {instrument.SerialNumber}...");
        var connectionResult = await unify.ConnectInstrument(instrument.SerialNumber);

        if (connectionResult != ConnectionResult.Success)
        {
            throw new InvalidOperationException($"Connecting to {instrument.SerialNumber} failed with result {connectionResult}.");
        }

        connectedInstrumentSerialNumber = instrument.SerialNumber;

        // Read the connected instrument definition so the trace uses settings
        // that are valid for the specific hardware model.
        if (unify.GetInstrumentDefinition(instrument.SerialNumber) is not CaaInstrumentDefinition instrumentDefinition)
        {
            throw new InvalidOperationException("The connected instrument did not provide a CAA instrument definition.");
        }

        // Configure a single return loss vs frequency trace so the SDK can
        // tell us which OSL calibration is required for this measurement.
        var test = new Test
        {
            Traces =
            [
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
            ],
            Cardinality = Cardinality.Single,
        };

        // Configure the trace before calibrating so the SDK can validate the
        // request and return the matching calibration requirements.
        var testConfigurationResult = unify.ConfigureTest(test);

        if (!testConfigurationResult.Valid)
        {
            throw new InvalidOperationException("The SDK rejected the return loss test configuration.");
        }

        if (testConfigurationResult.TraceConfigurationResults.FirstOrDefault() is not CaaReturnLossFrequencyTraceConfigurationResult traceConfiguration)
        {
            throw new InvalidOperationException("The SDK did not return a CAA return loss frequency trace configuration result.");
        }

        if (traceConfiguration.Calibration is not OslCalibration calibration)
        {
            throw new InvalidOperationException("The configured trace did not include a required OSL calibration.");
        }

        // Show the user which calibration kit the SDK selected from the trace
        // configuration result.
        Console.WriteLine($"Configured return loss trace for {instrumentDefinition.Model}");
        Console.WriteLine($"Using calibration kit: {calibration.CalKitName}");

        // Configure the calibration itself before starting the step-by-step
        // workflow.
        if (unify.ConfigureCalibration(calibration) is not OslCalibrationConfigurationResult calibrationConfigurationResult)
        {
            throw new InvalidOperationException("The SDK did not return an OSL calibration configuration result.");
        }

        if (calibrationConfigurationResult.ValidationErrors.Count > 0)
        {
            var validationErrors = string.Join(", ", calibrationConfigurationResult.ValidationErrors.Select(error => error.Type));
            throw new InvalidOperationException($"The SDK rejected the OSL calibration configuration: {validationErrors}.");
        }

        Console.WriteLine($"Calibration configured for {FormatFrequencyRange(calibration.FrequencyRange_Hz)}.");

        string? lastStartedStepId = null;

        // RunCalibration streams the current calibration state. Each update
        // tells us which step is ready, which ones are complete, and what
        // instructions to show before the next step runs.
        await foreach (var calibrationState in unify.RunCalibration())
        {
            if (calibrationState is not OslCalibrationState oslState)
            {
                continue;
            }

            if (oslState.Steps.All(step => step.Complete))
            {
                Console.WriteLine("Calibration complete!");
                break;
            }

            Console.WriteLine();
            Console.WriteLine("Calibration state:");

            foreach (var step in oslState.Steps)
            {
                var status = step.Complete ? "complete" : step.Enabled ? "ready" : "waiting";
                Console.WriteLine($"- {step.Name}: {status}");
            }

            var nextStep = oslState.Steps.FirstOrDefault(step => step.Enabled && !step.Complete);

            if (nextStep is null || nextStep.Id == lastStartedStepId)
            {
                continue;
            }

            lastStartedStepId = nextStep.Id;

            Console.WriteLine();
            Console.WriteLine($"Next step: {nextStep.Name}");
            Console.WriteLine(nextStep.Instruction);
            Console.WriteLine("Press Enter to run this step.");
            Console.ReadLine();

            // Each calibration step reports its own progress separately from
            // the overall calibration state stream.
            await foreach (var progress in unify.RunCalibrationStep(nextStep.Id))
            {
                if (progress < 0)
                {
                    Console.WriteLine("Step progress: indeterminate");
                }
                else
                {
                    var percentage = (int)Math.Round(progress * 100);
                    Console.WriteLine($"Step progress: {percentage}%");
                }
            }

            Console.WriteLine($"Completed step: {nextStep.Name}");
        }
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

static async Task<Instrument> DiscoverFirstIwa(Unify unify)
{
    Instrument? instrument = null;

    // Give discovery an explicit timeout so the example fails clearly
    // instead of waiting forever when no iWA is nearby.
    using var discoveryTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));

    try
    {
        // RunBluetoothScan stays active while the SDK is scanning, so we keep
        // the returned task alive in the background while discovery waits for
        // an instrument to appear.
        _ = unify.RunBluetoothScan();

        await foreach (var instruments in unify.RunInstrumentDiscovery(discoveryTimeout.Token))
        {
            instrument = instruments.FirstOrDefault(candidate => candidate.Type == InstrumentType.Iwa);

            if (instrument is not null)
            {
                return instrument;
            }
        }
    }
    catch (OperationCanceledException) when (discoveryTimeout.IsCancellationRequested)
    {
        throw new System.TimeoutException("Timed out waiting for an iWA instrument to be discovered.");
    }
    finally
    {
        // Stopping the scan is what tells the SDK to end Bluetooth discovery.
        await unify.StopBluetoothScan();
    }

    throw new InvalidOperationException("Instrument discovery completed without finding an iWA instrument.");
}

static string FormatFrequencyRange(Extent range) =>
    $"{Math.Round(range.Start):F0} Hz to {Math.Round(range.End):F0} Hz";
