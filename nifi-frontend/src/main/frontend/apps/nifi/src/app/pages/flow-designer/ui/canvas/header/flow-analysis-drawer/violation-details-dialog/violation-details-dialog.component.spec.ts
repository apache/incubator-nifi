import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ViolationDetailsDialogComponent } from './violation-details-dialog.component';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { provideMockStore } from '@ngrx/store/testing';

describe('ViolationDetailsDialogComponent', () => {
    let component: ViolationDetailsDialogComponent;
    let fixture: ComponentFixture<ViolationDetailsDialogComponent>;

    beforeEach(async () => {
        await TestBed.configureTestingModule({
            imports: [ViolationDetailsDialogComponent],
            providers: [
                {
                    provide: MAT_DIALOG_DATA,
                    useValue: {
                        violation: {
                            enforcementPolicy: 'ENFORCE',
                            scope: '4b4cfdf2-0190-1000-500d-b170055a0a34',
                            subjectId: '4b4cfdf2-0190-1000-500d-b170055a0a34',
                            subjectDisplayName: 'AttributeRollingWindow',
                            groupId: '36890551-0190-1000-cfaa-27b854604d18',
                            ruleId: '369c8d4e-0190-1000-99b5-8c4e0144f1a9',
                            issueId: 'default',
                            violationMessage: "'AttributeRollingWindow' is not allowed",
                            subjectComponentType: 'PROCESSOR',
                            subjectPermissionDto: {
                                canRead: true,
                                canWrite: true
                            },
                            enabled: false
                        },
                        rule: {
                            id: '369c8d4e-0190-1000-99b5-8c4e0144f1a9',
                            name: 'DisallowComponentType',
                            type: 'org.apache.nifi.flowanalysis.rules.DisallowComponentType',
                            bundle: {
                                group: 'org.apache.nifi',
                                artifact: 'nifi-standard-nar',
                                version: '2.0.0-SNAPSHOT'
                            },
                            state: 'ENABLED',
                            persistsState: false,
                            restricted: false,
                            deprecated: false,
                            multipleVersionsAvailable: false,
                            supportsSensitiveDynamicProperties: false,
                            enforcementPolicy: 'ENFORCE',
                            properties: {
                                'component-type': 'AttributeRollingWindow'
                            },
                            descriptors: {
                                'component-type': {
                                    name: 'component-type',
                                    displayName: 'Component Type',
                                    description:
                                        "Components of the given type will produce a rule violation (i.e. they shouldn't exist). Either the simple or the fully qualified name of the type should be provided.",
                                    required: true,
                                    sensitive: false,
                                    dynamic: false,
                                    supportsEl: false,
                                    expressionLanguageScope: 'Not Supported',
                                    dependencies: []
                                }
                            },
                            validationStatus: 'VALID',
                            extensionMissing: false
                        }
                    }
                },
                {
                    provide: MatDialogRef,
                    useValue: {}
                },
                provideMockStore({})
            ]
        }).compileComponents();

        fixture = TestBed.createComponent(ViolationDetailsDialogComponent);
        component = fixture.componentInstance;
        fixture.detectChanges();
    });

    it('should create', () => {
        expect(component).toBeTruthy();
    });
});
