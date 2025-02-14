/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
import org.apache.nifi.annotation.notification.PrimaryNodeState;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.controller.AbstractComponentNode;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.ValidationContextFactory;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.migration.ControllerServiceCreationDetails;
import org.apache.nifi.migration.ControllerServiceFactory;
import org.apache.nifi.migration.StandardPropertyConfiguration;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.util.CharacterFilterUtils;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.ReflectionUtils;
import org.apache.nifi.util.Tuple;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class StandardControllerServiceNode extends AbstractComponentNode implements ControllerServiceNode {

    private static final Logger LOG = LoggerFactory.getLogger(StandardControllerServiceNode.class);


    private final AtomicReference<ControllerServiceDetails> controllerServiceHolder = new AtomicReference<>(null);
    private final ControllerServiceProvider serviceProvider;
    private final ServiceStateTransition stateTransition;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();

    private final Set<Tuple<ComponentNode, PropertyDescriptor>> referencingComponents = new HashSet<>();
    private volatile String comment;
    private volatile ProcessGroup processGroup;
    private volatile LogLevel bulletinLevel = LogLevel.WARN;

    private final AtomicBoolean active;
    private final AtomicBoolean moving;

    public StandardControllerServiceNode(final LoggableComponent<ControllerService> implementation, final LoggableComponent<ControllerService> proxiedControllerService,
                                         final ControllerServiceInvocationHandler invocationHandler, final String id, final ValidationContextFactory validationContextFactory,
                                         final ControllerServiceProvider serviceProvider, final ReloadComponent reloadComponent,
                                         final ExtensionManager extensionManager, final ValidationTrigger validationTrigger) {

        this(implementation, proxiedControllerService, invocationHandler, id, validationContextFactory, serviceProvider, implementation.getComponent().getClass().getSimpleName(),
            implementation.getComponent().getClass().getCanonicalName(), reloadComponent, extensionManager, validationTrigger, false);
    }

    public StandardControllerServiceNode(final LoggableComponent<ControllerService> implementation, final LoggableComponent<ControllerService> proxiedControllerService,
                                         final ControllerServiceInvocationHandler invocationHandler, final String id, final ValidationContextFactory validationContextFactory,
                                         final ControllerServiceProvider serviceProvider, final String componentType, final String componentCanonicalClass,
                                         final ReloadComponent reloadComponent, final ExtensionManager extensionManager,
                                         final ValidationTrigger validationTrigger, final boolean isExtensionMissing) {

        super(id, validationContextFactory, serviceProvider, componentType, componentCanonicalClass, reloadComponent, extensionManager, validationTrigger, isExtensionMissing);
        this.serviceProvider = serviceProvider;
        this.active = new AtomicBoolean();
        this.moving = new AtomicBoolean(false);
        setControllerServiceAndProxy(implementation, proxiedControllerService, invocationHandler);
        stateTransition = new ServiceStateTransition(this);
        this.comment = "";
    }

    @Override
    public ConfigurableComponent getComponent() {
        return controllerServiceHolder.get().getImplementation();
    }

    @Override
    public TerminationAwareLogger getLogger() {
        return controllerServiceHolder.get().getComponentLog();
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return controllerServiceHolder.get().getBundleCoordinate();
    }

    @Override
    public Authorizable getParentAuthorizable() {
        final ProcessGroup processGroup = getProcessGroup();
        if (processGroup == null) {
            return new Authorizable() {
                @Override
                public Authorizable getParentAuthorizable() {
                    return null;
                }

                @Override
                public Resource getResource() {
                    return ResourceFactory.getControllerResource();
                }
            };
        } else {
            return processGroup;
        }
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ControllerService, getIdentifier(), getName());
    }

    @Override
    public boolean isRestricted() {
        return getControllerServiceImplementation().getClass().isAnnotationPresent(Restricted.class);
    }

    @Override
    public Class<?> getComponentClass() {
        return getControllerServiceImplementation().getClass();
    }

    @Override
    public boolean isDeprecated() {
        return getControllerServiceImplementation().getClass().isAnnotationPresent(DeprecationNotice.class);
    }

    @Override
    public ControllerService getControllerServiceImplementation() {
        return controllerServiceHolder.get().getImplementation();
    }

    @Override
    public ControllerService getProxiedControllerService() {
        return controllerServiceHolder.get().getProxiedControllerService();
    }

    @Override
    public ControllerServiceInvocationHandler getInvocationHandler() {
        return controllerServiceHolder.get().getInvocationHandler();
    }

    @Override
    public void setControllerServiceAndProxy(final LoggableComponent<ControllerService> implementation,
                                             final LoggableComponent<ControllerService> proxiedControllerService,
                                             final ControllerServiceInvocationHandler invocationHandler) {
        synchronized (this.active) {
            if (isActive()) {
                throw new IllegalStateException("Cannot modify configuration of " + this + " while service is active");
            }

            final ControllerServiceDetails controllerServiceDetails = new ControllerServiceDetails(implementation, proxiedControllerService, invocationHandler);
            this.controllerServiceHolder.set(controllerServiceDetails);
        }
    }

    @Override
    public void reload(final Set<URL> additionalUrls) throws ControllerServiceInstantiationException {
        synchronized (this.active) {
            final String additionalResourcesFingerprint = ClassLoaderUtils.generateAdditionalUrlsFingerprint(additionalUrls, determineClasloaderIsolationKey());
            setAdditionalResourcesFingerprint(additionalResourcesFingerprint);
            getReloadComponent().reload(this, getCanonicalClassName(), getBundleCoordinate(), additionalUrls);
        }
    }

    @Override
    public void setProperties(final Map<String, String> properties, final boolean allowRemovalOfRequiredProperties, final Set<String> sensitiveDynamicPropertyNames) {
        super.setProperties(properties, allowRemovalOfRequiredProperties, sensitiveDynamicPropertyNames);

        // It's possible that changing the properties of this Controller Service could alter the Classloader Isolation Key of a referencing
        // component so reload any referencing component as necessary.
        getReferences().findRecursiveReferences(ComponentNode.class).forEach(ComponentNode::reloadAdditionalResourcesIfNecessary);
    }

    @Override
    public ProcessGroup getProcessGroup() {
        return processGroup;
    }

    public Optional<ProcessGroup> getParentProcessGroup() {
        return Optional.ofNullable(this.processGroup);
    }

    @Override
    public void setProcessGroup(final ProcessGroup group) {
        this.processGroup = group;
        LOG.debug("Resetting Validation State of {} due to setting process group", this);
        resetValidationState();
    }

    @Override
    public ControllerServiceReference getReferences() {
        readLock.lock();
        try {
            // In case a controller service is referenced multiple times by a component node, the latter is decoupled here
            return new StandardControllerServiceReference(this, referencingComponents.stream().map(Tuple::getKey).collect(Collectors.toSet()));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void addReference(final ComponentNode referencingComponent, final PropertyDescriptor propertyDescriptor) {
        writeLock.lock();
        try {
            final boolean added = referencingComponents.add(new Tuple<>(referencingComponent, propertyDescriptor));

            if (added) {
                LOG.debug("{} Added referencing component {} for property {}", this, referencingComponent, propertyDescriptor.getName());
            } else {
                LOG.debug("{} Will not add referencing component {} for property {} because there is already a reference", this, referencingComponent, propertyDescriptor.getName());
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void updateReference(final ComponentNode referencingComponent, final PropertyDescriptor propertyDescriptor) {
        writeLock.lock();
        try {
            Tuple<ComponentNode, PropertyDescriptor> updatedTuple = new Tuple<>(referencingComponent, propertyDescriptor);

            // Check to see if there is any reference for the given component and property descriptor. If there is, we want to use
            // the existing component object instead of the newly provided one. This is done because when a Stateless Process Group is started,
            // the instance that is "ethereal" in the stateless engine may call updateReference, and we do not want to change the reference to the
            // component to this ethereal instance but instead want to hold onto the existing component.
            for (final Tuple<ComponentNode, PropertyDescriptor> tuple : referencingComponents) {
                if (Objects.equals(tuple.getKey(), referencingComponent)) {
                    updatedTuple = new Tuple<>(tuple.getKey(), propertyDescriptor);
                    LOG.debug("{} updating reference from component {} and property {}", this, referencingComponent, propertyDescriptor);
                }
            }

            referencingComponents.remove(updatedTuple);
            referencingComponents.add(updatedTuple);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected ParameterContext getParameterContext() {
        final ProcessGroup processGroup = getProcessGroup();
        return processGroup == null ? null : processGroup.getParameterContext();
    }


    @Override
    public List<ControllerServiceNode> getRequiredControllerServices() {
        Set<ControllerServiceNode> requiredServices = new HashSet<>();
        for (Entry<PropertyDescriptor, String> entry : getEffectivePropertyValues().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null && entry.getValue() != null) {
                // CS property could point to a non-existent CS, so protect against requiredNode being null
                final String referenceId = entry.getValue();
                final ControllerServiceNode requiredNode = serviceProvider.getControllerServiceNode(referenceId);
                if (requiredNode != null) {
                    requiredServices.add(requiredNode);
                } else {
                    LOG.warn("Unable to locate referenced controller service with id {}", referenceId);
                }
            }
        }
        return new ArrayList<>(requiredServices);
    }


    @Override
    public void removeReference(final ComponentNode referencingComponent, final PropertyDescriptor propertyDescriptor) {
        writeLock.lock();
        try {
            referencingComponents.remove(new Tuple<>(referencingComponent, propertyDescriptor));
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void verifyModifiable() throws IllegalStateException {
        final ControllerServiceState state = getState();

        if (state == ControllerServiceState.DISABLING) {
            // Provide precise/accurate error message for DISABLING case
            throw new IllegalStateException("Cannot modify configuration of " + this + " because it is currently still disabling. " +
                "Please wait for the service to fully disable before attempting to modify it.");
        }
        if (state != ControllerServiceState.DISABLED) {
            throw new IllegalStateException("Cannot modify configuration of " + this + " because it is currently not disabled - it has a state of " + state
                + ". Please disable the Controller Service first.");
        }
    }

    @Override
    public void verifyCanDelete() {
        if (getState() != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(this + " cannot be deleted because it is not disabled");
        }
    }

    @Override
    public void verifyCanDisable() {
        verifyCanDisable(Collections.emptySet());
    }

    @Override
    public void verifyCanDisable(final Set<ControllerServiceNode> ignoreReferences) {
        if (!this.isActive()) {
            return;
        }

        final ControllerServiceReference references = getReferences();

        final Set<String> activeReferencesIdentifiers = new HashSet<>();
        for (final ComponentNode activeReference : references.getActiveReferences()) {
            if (!ignoreReferences.contains(activeReference)) {
                activeReferencesIdentifiers.add(activeReference.getIdentifier());
            }
        }

        if (!activeReferencesIdentifiers.isEmpty()) {
            throw new IllegalStateException(this + " cannot be disabled because it is referenced by " + activeReferencesIdentifiers.size() +
                " components that are currently running: [" + StringUtils.join(activeReferencesIdentifiers, ", ") + "]");
        }
    }

    @Override
    public void verifyCanEnable() {
        final ControllerServiceState state = getState();
        switch (state) {
            case DISABLED:
                return;
            case DISABLING:
                throw new IllegalStateException(this + " cannot be enabled because it is not disabled - it has a state of " + state);
            default:
                if (isReloadAdditionalResourcesNecessary()) {
                    throw new IllegalStateException(this + " cannot be enabled because additional resources are needed - it has a state of " + state);
                }
        }
    }

    @Override
    public void verifyCanEnable(final Set<ControllerServiceNode> ignoredReferences) {
        verifyCanEnable();
    }

    @Override
    public void verifyCanUpdate() {
        final ControllerServiceState state = getState();
        if (state != ControllerServiceState.DISABLED) {
            throw new IllegalStateException(this + " cannot be updated because it is not disabled - it has a state of " + state);
        }
    }

    @Override
    public void verifyCanClearState() {
        verifyCanUpdate();
    }

    @Override
    public String getComments() {
        return comment;
    }

    @Override
    public void setComments(final String comment) {
        this.comment = CharacterFilterUtils.filterInvalidXmlCharacters(comment);
    }

    @Override
    public ControllerServiceState getState() {
        return stateTransition.getState();
    }

    @Override
    public boolean isActive() {
        return this.active.get();
    }
    @Override
    public boolean isMoving() {
        return this.moving.get();
    }
    @Override
    public void setMoving(boolean moving) {
        this.moving.set(moving);
    }
    @Override
    public boolean awaitEnabled(final long timePeriod, final TimeUnit timeUnit) throws InterruptedException {
        LOG.debug("Waiting up to {} {} for {} to be enabled", timePeriod, timeUnit, this);
        final boolean enabled = stateTransition.awaitStateOrInvalid(ControllerServiceState.ENABLED, timePeriod, timeUnit);

        if (enabled) {
            LOG.debug("{} is enabled", this);
        } else {
            LOG.debug("After {} {}, {} is NOT enabled", timePeriod, timeUnit, this);
        }

        return enabled;
    }

    @Override
    public boolean awaitDisabled(final long timePeriod, final TimeUnit timeUnit) throws InterruptedException {
        LOG.debug("Waiting up to {} {} for {} to be disabled", timePeriod, timeUnit, this);
        final boolean disabled = stateTransition.awaitState(ControllerServiceState.DISABLED, timePeriod, timeUnit);

        if (disabled) {
            LOG.debug("{} is now disabled", this);
        } else {
            LOG.debug("After {} {}, {} is NOT disabled", timePeriod, timeUnit, this);
        }

        return disabled;
    }

    @Override
    public void verifyCanPerformVerification() {
        final ControllerServiceState state = getState();
        if (state != ControllerServiceState.DISABLED) {
            throw new IllegalStateException("Cannot perform verification because the " + this + " is not disabled - it has a state of " + state);
        }
    }

    @Override
    public List<ConfigVerificationResult> verifyConfiguration(final ConfigurationContext context, final ComponentLog logger, final Map<String, String> variables,
                                                              final ExtensionManager extensionManager) {

        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            verifyCanPerformVerification();

            final long startNanos = System.nanoTime();
            // Call super's verifyConfig, which will perform component validation
            results.addAll(super.verifyConfig(context.getProperties(), context.getAnnotationData(), getProcessGroup() == null ? null : getProcessGroup().getParameterContext()));
            final long validationComplete = System.nanoTime();

            // If any invalid outcomes from validation, we do not want to perform additional verification, because we only run additional verification when the component is valid.
            // This is done in order to make it much simpler to develop these verifications, since the developer doesn't have to worry about whether or not the given values are valid.
            if (!results.isEmpty() && results.stream().anyMatch(result -> result.getOutcome() == Outcome.FAILED)) {
                return results;
            }

            final ControllerService controllerService = getControllerServiceImplementation();
            if (controllerService instanceof VerifiableControllerService) {
                LOG.debug("{} is a VerifiableControllerService. Will perform full verification of configuration.", this);

                final VerifiableControllerService verifiable = (VerifiableControllerService) controllerService;

                // Check if the given configuration requires a different classloader than the current configuration
                final boolean classpathDifferent = isClasspathDifferent(context.getProperties());

                if (classpathDifferent) {
                    // Create a classloader for the given configuration and use that to verify the component's configuration
                    final Bundle bundle = extensionManager.getBundle(getBundleCoordinate());
                    final Set<URL> classpathUrls = getAdditionalClasspathResources(context.getProperties().keySet(), descriptor -> context.getProperty(descriptor).getValue());

                    final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
                    final String classLoaderIsolationKey = getClassLoaderIsolationKey(context);
                    try (final InstanceClassLoader detectedClassLoader = extensionManager.createInstanceClassLoader(getComponentType(), getIdentifier(), bundle, classpathUrls, false,
                                classLoaderIsolationKey)) {
                        Thread.currentThread().setContextClassLoader(detectedClassLoader);
                        results.addAll(verifiable.verify(context, logger, variables));
                    } finally {
                        Thread.currentThread().setContextClassLoader(currentClassLoader);
                    }
                } else {
                    // Verify the configuration, using the component's classloader
                    try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, controllerService.getClass(), getIdentifier())) {
                        results.addAll(verifiable.verify(context, logger, variables));
                    }
                }

                final long validationNanos = validationComplete - startNanos;
                final long verificationNanos = System.nanoTime() - validationComplete;
                LOG.debug("{} completed full configuration validation in {} plus {} for validation",
                    this, FormatUtils.formatNanos(verificationNanos, false), FormatUtils.formatNanos(validationNanos, false));
            } else {
                LOG.debug("{} is not a VerifiableControllerService, so will not perform full verification of configuration. Validation took {}", this,
                    FormatUtils.formatNanos(validationComplete - startNanos, false));
            }
        } catch (final Throwable t) {
            LOG.error("Failed to perform verification of Controller Service's configuration for {}", this, t);

            results.add(new ConfigVerificationResult.Builder()
                .outcome(Outcome.FAILED)
                .verificationStepName("Perform Verification")
                .explanation("Encountered unexpected failure when attempting to perform verification: " + t)
                .build());
        }

        return results;
    }

    @Override
    public boolean isValidationNecessary() {
        return switch (getState()) {
            case DISABLED, DISABLING -> true;
            case ENABLING ->
                // If enabling and currently not valid, then we must trigger validation to occur. This allows the #enable method
                // to continue running in the background and complete enabling when the service becomes valid.
                    getValidationStatus() != ValidationStatus.VALID;
            default -> false;
        };
    }

    @Override
    public ValidationState performValidation(final ValidationContext validationContext) {
        final ValidationState state = super.performValidation(validationContext);
        if (state.getStatus() == ValidationStatus.INVALID) {
            stateTransition.signalInvalid();
        }
        return state;
    }

    @Override
    protected List<ValidationResult> validateConfig() {
        return Collections.emptyList();
    }

    /**
     * Will atomically enable this service by invoking its @OnEnabled operation.
     * It uses CAS operation on {@link #stateTransition} to transition this service
     * from DISABLED to ENABLING state. If such transition succeeds the service
     * will be marked as 'active' (see {@link ControllerServiceNode#isActive()}).
     * If such transition doesn't succeed then no enabling logic will be
     * performed and the method will exit. In other words it is safe to invoke
     * this operation multiple times and from multiple threads.
     * <br>
     * This operation will also perform re-try of service enabling in the event
     * of exception being thrown by previous invocation of @OnEnabled.
     * <br>
     * Upon successful invocation of @OnEnabled this service will be transitioned to
     * ENABLED state.
     * <br>
     * In the event where enabling took longer then expected by the user and such user
     * initiated disable operation, this service will be automatically disabled as soon
     * as it reached ENABLED state.
     */
    @Override
    public CompletableFuture<Void> enable(final ScheduledExecutorService scheduler, final long administrativeYieldMillis) {
        final CompletableFuture<Void> future = new CompletableFuture<>();

        if (this.stateTransition.transitionToEnabling(ControllerServiceState.DISABLED, future)) {
            synchronized (active) {
                this.active.set(true);
            }

            final ControllerServiceProvider controllerServiceProvider = this.serviceProvider;
            final StandardControllerServiceNode service = this;
            AtomicLong enablingAttemptCount = new AtomicLong(0);
            scheduler.execute(new Runnable() {
                @Override
                public void run() {
                    final ConfigurationContext configContext = new StandardConfigurationContext(StandardControllerServiceNode.this, controllerServiceProvider, null);

                    if (!isActive()) {
                        LOG.warn("Enabling {} stopped: no active status", StandardControllerServiceNode.this);
                        stateTransition.disable();
                        future.complete(null);
                        return;
                    }

                    final ValidationStatus validationStatus = getValidationStatus();
                    if (validationStatus != ValidationStatus.VALID) {
                        final ValidationState validationState = getValidationState();
                        LOG.debug("Enabling {} failed: Validation Status [{}] Errors {} Attempt [{}] Retrying...",
                            StandardControllerServiceNode.this, validationStatus, validationState.getValidationErrors(), enablingAttemptCount.get());

                        enablingAttemptCount.incrementAndGet();
                        if (enablingAttemptCount.get() == 120 || enablingAttemptCount.get() % 3600 == 0) {
                            final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this,
                                    new StandardLoggingContext(StandardControllerServiceNode.this));
                            componentLog.error("Enabling {} failed: Validation Status [{}] Errors {}",
                                    service, validationStatus, validationState.getValidationErrors());
                        }

                        try {
                            scheduler.schedule(this, 1, TimeUnit.SECONDS);
                        } catch (RejectedExecutionException rejectedExecutionException) {
                            LOG.error("Enabling {} failed: Validation Status [{}] Errors {}", StandardControllerServiceNode.this, validationStatus, validationState.getValidationErrors(),
                                    rejectedExecutionException);
                        }
                        future.complete(null);
                        return;
                    }

                    try {
                        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), getControllerServiceImplementation().getClass(), getIdentifier())) {
                            ReflectionUtils.invokeMethodsWithAnnotation(OnEnabled.class, getControllerServiceImplementation(), configContext);
                        }

                        boolean shouldEnable;
                        synchronized (active) {
                            shouldEnable = active.get() && stateTransition.enable(getReferences()); // Transitioning the state to ENABLED will complete our future.
                        }

                        if (!shouldEnable) {
                            LOG.info("Disabling {} after enabled due to disable action initiated", service);
                            // Can only happen if user initiated DISABLE operation before service finished enabling. It's state will be
                            // set to DISABLING (see disable() operation)
                            invokeDisable(configContext);
                            stateTransition.disable();
                            future.complete(null);
                        } else {
                            LOG.info("Enabled {}", service);
                        }
                    } catch (Exception e) {
                        future.completeExceptionally(e);

                        final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
                        final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this,
                                new StandardLoggingContext(StandardControllerServiceNode.this));
                        componentLog.error("Failed to invoke @OnEnabled method", cause);
                        invokeDisable(configContext);

                        if (isActive()) {
                            scheduler.schedule(this, administrativeYieldMillis, TimeUnit.MILLISECONDS);
                        } else {
                            stateTransition.disable();
                        }
                    }
                }
            });
        } else {
            future.complete(null);
        }

        return future;
    }


    /**
     * Will atomically disable this service by invoking its @OnDisabled operation.
     * It uses CAS operation on {@link #stateTransition} to transition this service
     * from ENABLED to DISABLING state. If such transition succeeds the service
     * will be de-activated (see {@link ControllerServiceNode#isActive()}).
     * If such transition doesn't succeed (the service is still in ENABLING state)
     * then the service will still be transitioned to DISABLING state to ensure that
     * no other transition could happen on this service. However in such event
     * (e.g., its @OnEnabled finally succeeded), the {@link #enable(ScheduledExecutorService, long)}
     * operation will initiate service disabling javadoc for (see {@link #enable(ScheduledExecutorService, long)}
     * <br>
     * Upon successful invocation of @OnDisabled this service will be transitioned to
     * DISABLED state.
     */
    @Override
    public CompletableFuture<Void> disable(ScheduledExecutorService scheduler) {
        /*
         * The reason for synchronization is to ensure consistency of the
         * service state when another thread is in the middle of enabling this
         * service since it will attempt to transition service state from
         * ENABLING to ENABLED but only if it's active.
         */
        synchronized (this.active) {
            this.active.set(false);
        }

        final CompletableFuture<Void> future = new CompletableFuture<>();
        final boolean transitioned = this.stateTransition.transitionToDisabling(ControllerServiceState.ENABLING, future);
        if (transitioned) {
            return future;
        }

        if (this.stateTransition.transitionToDisabling(ControllerServiceState.ENABLED, future)) {
            final ConfigurationContext configContext = new StandardConfigurationContext(this, this.serviceProvider, null);
            scheduler.execute(() -> {
                try {
                    invokeDisable(configContext);
                } finally {
                    stateTransition.disable();

                    // Now all components that reference this service will be invalid. Trigger validation to occur so that
                    // this is reflected in any response that may go back to a user/client.
                    for (final ComponentNode component : getReferences().getReferencingComponents()) {
                        component.performValidation();
                    }
                }
            });
        }

        return future;
    }


    private void invokeDisable(ConfigurationContext configContext) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), getControllerServiceImplementation().getClass(), getIdentifier())) {
            ReflectionUtils.invokeMethodsWithAnnotation(OnDisabled.class, StandardControllerServiceNode.this.getControllerServiceImplementation(), configContext);
            LOG.debug("Successfully disabled {}", this);
        } catch (Exception e) {
            final Throwable cause = e instanceof InvocationTargetException ? e.getCause() : e;
            final ComponentLog componentLog = new SimpleProcessLogger(getIdentifier(), StandardControllerServiceNode.this, new StandardLoggingContext(StandardControllerServiceNode.this));
            componentLog.error("Failed to invoke @OnDisabled method due to {}", cause);
            LOG.error("Failed to invoke @OnDisabled method of {} due to {}", getControllerServiceImplementation(), cause.toString());
        }
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.ofNullable(versionedComponentId.get());
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
        boolean updated = false;
        while (!updated) {
            final String currentId = this.versionedComponentId.get();

            if (currentId == null) {
                updated = this.versionedComponentId.compareAndSet(null, versionedComponentId);
            } else if (currentId.equals(versionedComponentId)) {
                return;
            } else if (versionedComponentId == null) {
                updated = this.versionedComponentId.compareAndSet(currentId, null);
            } else {
                throw new IllegalStateException(this + " is already under version control");
            }
        }
    }

    @Override
    public String toString() {
        return "StandardControllerServiceNode[" +
                "service=" + super.toString() +
                ", name=" + getName() +
                ", active=" + active +
                "]";
    }

    @Override
    public ParameterLookup getParameterLookup() {
        return getParameterContext();
    }


    @Override
    public LogLevel getBulletinLevel() {
        return bulletinLevel;
    }

    @Override
    public synchronized void setBulletinLevel(LogLevel level) {
        // handling backward compatibility with nifi 1.16 and earlier when bulletinLevel did not exist in flow.xml/flow.json
        // and bulletins were always logged at WARN level
        if (level == null) {
            level = LogLevel.WARN;
        }

        LogRepositoryFactory.getRepository(getIdentifier()).setObservationLevel(level);
        this.bulletinLevel = level;
    }

    @Override
    public void notifyPrimaryNodeChanged(final PrimaryNodeState nodeState) {
        final Class<?> implementationClass = getControllerServiceImplementation().getClass();
        final List<Method> methods = ReflectionUtils.findMethodsWithAnnotations(implementationClass, new Class[] {OnPrimaryNodeStateChange.class});
        if (methods.isEmpty()) {
            return;
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), implementationClass, getIdentifier())) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnPrimaryNodeStateChange.class, getControllerServiceImplementation(), nodeState);
        }
    }

    @Override
    public void migrateConfiguration(final Map<String, String> originalPropertyValues, final ControllerServiceFactory serviceFactory) {
        final Map<String, String> effectiveValues = new HashMap<>();
        originalPropertyValues.forEach((key, value) -> effectiveValues.put(key, mapRawValueToEffectiveValue(value)));

        final StandardPropertyConfiguration propertyConfig = new StandardPropertyConfiguration(effectiveValues,
                originalPropertyValues, super::mapRawValueToEffectiveValue, toString(), serviceFactory);

        final ControllerService implementation = getControllerServiceImplementation();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(getExtensionManager(), implementation.getClass(), getIdentifier())) {
            implementation.migrateProperties(propertyConfig);
        } catch (final Exception e) {
            LOG.error("Failed to migrate Property Configuration for {}.", this, e);
        }

        if (propertyConfig.isModified()) {
            // Create any necessary Controller Services. It is important that we create the services
            // before updating this service's properties, as it's necessary in order to properly account
            // for the Controller Service References.
            final List<ControllerServiceCreationDetails> servicesCreated = propertyConfig.getCreatedServices();
            servicesCreated.forEach(serviceFactory::create);

            overwriteProperties(propertyConfig.getRawProperties());
        }
    }

    @Override
    protected void performFlowAnalysisOnThis() {
        getValidationContextFactory().getFlowAnalyzer().ifPresent(flowAnalyzer -> flowAnalyzer.analyzeControllerService(this));
    }
}
